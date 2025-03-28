package project_2


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.rdd._


object main{

  val seed = new java.util.Date().hashCode;
  val rand = new scala.util.Random(seed);

  class hash_function(numBuckets_in: Long) extends Serializable {  // a 2-universal hash family, numBuckets_in is the numer of buckets
    val p: Long = 2147483587;  // p is a prime around 2^31 so the computation will fit into 2^63 (Long)
    val a: Long = (rand.nextLong %(p-1)) + 1  // a is a random number is [1,p]
    val b: Long = (rand.nextLong % p) // b is a random number in [0,p]
    val numBuckets: Long = numBuckets_in

    def convert(s: String, ind: Int): Long = {
      if(ind==0)
        return 0;
      return (s(ind-1).toLong + 256 * (convert(s,ind-1))) % p;
    }

    def hash(s: String): Long = {
      return ((a * convert(s,s.length) + b) % p) % numBuckets;
    }

    def hash(t: Long): Long = {
      return ((a * t + b) % p) % numBuckets;
    }

    def zeroes(num: Long, remain: Long): Int =
    {
      if((num & 1) == 1 || remain==1)
        return 0;
      return 1+zeroes(num >> 1, remain >> 1);
    }

    def zeroes(num: Long): Int =        /*calculates #consecutive trialing zeroes  */
    {
      return zeroes(num, numBuckets)
    }
  }

  class four_universal_Radamacher_hash_function extends hash_function(2) {  // a 4-universal hash family, numBuckets_in is the numer of buckets
    override val a: Long = (rand.nextLong % p)   // a is a random number is [0,p]
    override val b: Long = (rand.nextLong % p) // b is a random number in [0,p]
    val c: Long = (rand.nextLong % p)   // c is a random number is [0,p]
    val d: Long = (rand.nextLong % p) // d is a random number in [0,p]

    override def hash(s: String): Long = {     /* returns +1 or -1 with prob. 1/2 */
      val t= convert(s,s.length)
      val t2 = t*t % p
      val t3 = t2*t % p
      return if ( ( ((a * t3 + b* t2 + c*t + b) % p) & 1) == 0 ) 1 else -1;
    }

    override def hash(t: Long): Long = {       /* returns +1 or -1 with prob. 1/2 */
      val t2 = t*t % p
      val t3 = t2*t % p
      return if( ( ((a * t3 + b* t2 + c*t + b) % p) & 1) == 0 ) 1 else -1;
    }
  }

  // BJKST Sketch class: stores the k smallest normalized hash values.
  class BJKSTSketch(val bucketSize: Int) extends Serializable {
    // minSet holds the smallest normalized hash values (each in [0, 1)).
    var minSet: List[Double] = List.empty[Double]

    // Add an element using the provided hash function.
    def add(s: String, h: hash_function): Unit = {
      val r = h.hash(s).toDouble / h.p.toDouble  // Normalize to [0,1).
      // Insert r and keep only the smallest bucketSize values.
      minSet = (r :: minSet).sorted.take(bucketSize)
    }

    // Merge with another sketch.
    def merge(that: BJKSTSketch): BJKSTSketch = {
      val merged = new BJKSTSketch(bucketSize)
      merged.minSet = (this.minSet ++ that.minSet).sorted.take(bucketSize)
      merged
    }

    // Estimate the number of distinct elements.
    def estimate(): Double = {
      if (minSet.size < bucketSize || minSet.isEmpty) 0.0
      else bucketSize / minSet.last
    }
  }

def computeTrialEstimate(x: RDD[String], width: Int): Double = {
  val h = new hash_function(2147483587)
  val smallest = x
    .map(s => h.hash(s).toDouble / h.p.toDouble)
    .takeOrdered(width)

  if (smallest.length < width) 0.0
  else width.toDouble / smallest.last
}

def BJKST(x: RDD[String], width: Int, trials: Int): Double = {
  val estimates = (1 to trials)
    .map(_ => computeTrialEstimate(x, width))
    .filter(_ > 0)
    .sorted

  if (estimates.isEmpty) 0.0
  else {
    val mid = estimates.size / 2
    if (estimates.size % 2 == 1) estimates(mid)
    else (estimates(mid - 1) + estimates(mid)) / 2.0
  }
}


  def tidemark(x: RDD[String], trials: Int): Double = {
    val h = Seq.fill(trials)(new hash_function(2000000000))

    def param0 = (accu1: Seq[Int], accu2: Seq[Int]) => Seq.range(0,trials).map(i => scala.math.max(accu1(i), accu2(i)))
    def param1 = (accu1: Seq[Int], s: String) => Seq.range(0,trials).map( i =>  scala.math.max(accu1(i), h(i).zeroes(h(i).hash(s))) )

    val x3 = x.aggregate(Seq.fill(trials)(0))( param1, param0)
    val ans = x3.map(z => scala.math.pow(2,0.5 + z)).sortWith(_ < _)( trials/2) /* Take the median of the trials */

    return ans
  }




  def Tug_of_War(x: RDD[String], width: Int, depth:Int) : Long = {
   
    val totalSketches = width * depth

    // Create a matrix of hash functions: depth rows and width columns
    val hashFuncs: Array[Array[String => Long]] = Array.tabulate(depth, width) { (d, w) =>
    new four_universal_Radamacher_hash_function().hash(_: String)
    }

    // Broadcast the matrix so each worker can use the same hash functions
    val broadcastHashFuncs = x.sparkContext.broadcast(hashFuncs)

    // First, compute the frequency for each distinct string
    val frequencies = x.map(s => (s, 1L)).reduceByKey(_ + _)

    val zero = Array.fill[Long](totalSketches)(0L)

    // For each distinct s and its frequency f, update all sketches by adding f * h(s)
    val sketchSums: Array[Long] = frequencies.aggregate(zero)(
      (acc, kv) => {
        val (s, f) = kv
        val funcs = broadcastHashFuncs.value
        var idx = 0
        for (d <- 0 until depth; w <- 0 until width) {
          // Multiply the frequency by the fixed hash value.
          acc(idx) += f * funcs(d)(w)(s)
          idx += 1
        }
        acc
      },
      (acc1, acc2) => {
        for (i <- 0 until totalSketches) {
          acc1(i) += acc2(i)
         }
        acc1
      }
    )
    
    // Square each sketch's sum to get an unbiased estimator for F2
    val squaredSketches = sketchSums.map(sum => sum * sum)

    // For each depth row, compute the mean of the squared values
    val rowMeans: Array[Double] = Array.tabulate(depth) { d =>
    val row = squaredSketches.slice(d * width, d * width + width)
    row.sum.toDouble / width
    }

    // Compute the median of the depth means
    val sortedMeans = rowMeans.sorted
    val median: Double = if (depth % 2 == 1) {
    sortedMeans(depth / 2)
    } else {
    (sortedMeans(depth / 2 - 1) + sortedMeans(depth / 2)) / 2.0
    }
    median.toLong
    }

  
  def exact_F0(x: RDD[String]) : Long = {
    val ans = x.distinct.count
    return ans
  }

  
  def exact_F2(x: RDD[String]) : Long = {
    // map each plate to a tuple with count 1, then sum them by plate
    // then map each plate count to its square, then add the squares
    val ans = x.map(i => (i,1L)).reduceByKey(_+_).map {case (_,count) => count*count}.reduce(_+_)
    return ans
  }


  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("Project_2").getOrCreate()

    if(args.length < 2) {
      println("Usage: project_2 input_path option = {BJKST, tidemark, ToW, exactF2, exactF0} ")
      sys.exit(1)
    }
    val input_path = args(0)

  //    val df = spark.read.format("csv").load("data/2014to2017.csv")
    val df = spark.read.format("csv").load(input_path)
    val dfrdd = df.rdd.map(row => row.getString(0))

    val startTimeMillis = System.currentTimeMillis()

    if(args(1)=="BJKST") {
      if (args.length != 4) {
        println("Usage: project_2 input_path BJKST #buckets trials")
        sys.exit(1)
      }
      val ans = BJKST(dfrdd, args(2).toInt, args(3).toInt)

      val endTimeMillis = System.currentTimeMillis()
      val durationSeconds = (endTimeMillis - startTimeMillis) / 1000

      println("==================================")
      println("BJKST Algorithm. Bucket Size:"+ args(2) + ". Trials:" + args(3) +". Time elapsed:" + durationSeconds + "s. Estimate: "+ans)
      println("==================================")
    }
    else if(args(1)=="tidemark") {
      if(args.length != 3) {
        println("Usage: project_2 input_path tidemark trials")
        sys.exit(1)
      }
      val ans = tidemark(dfrdd, args(2).toInt)
      val endTimeMillis = System.currentTimeMillis()
      val durationSeconds = (endTimeMillis - startTimeMillis) / 1000

      println("==================================")
      println("Tidemark Algorithm. Trials:" + args(2) + ". Time elapsed:" + durationSeconds + "s. Estimate: "+ans)
      println("==================================")

    }
    else if(args(1)=="ToW") {
       if(args.length != 4) {
         println("Usage: project_2 input_path ToW width depth")
         sys.exit(1)
      }
      val ans = Tug_of_War(dfrdd, args(2).toInt, args(3).toInt)
      val endTimeMillis = System.currentTimeMillis()
      val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
      println("==================================")
      println("Tug-of-War F2 Approximation. Width :" +  args(2) + ". Depth: "+ args(3) + ". Time elapsed:" + durationSeconds + "s. Estimate: "+ans)
      println("==================================")
    }
    else if(args(1)=="exactF2") {
      if(args.length != 2) {
        println("Usage: project_2 input_path exactF2")
        sys.exit(1)
      }
      val ans = exact_F2(dfrdd)
      val endTimeMillis = System.currentTimeMillis()
      val durationSeconds = (endTimeMillis - startTimeMillis) / 1000

      println("==================================")
      println("Exact F2. Time elapsed:" + durationSeconds + "s. Estimate: "+ans)
      println("==================================")
    }
    else if(args(1)=="exactF0") {
      if(args.length != 2) {
        println("Usage: project_2 input_path exactF0")
        sys.exit(1)
      }
      val ans = exact_F0(dfrdd)
      val endTimeMillis = System.currentTimeMillis()
      val durationSeconds = (endTimeMillis - startTimeMillis) / 1000

      println("==================================")
      println("Exact F0. Time elapsed:" + durationSeconds + "s. Estimate: "+ans)
      println("==================================")
    }
    else {
      println("Usage: project_2 input_path option = {BJKST, tidemark, ToW, exactF2, exactF0} ")
      sys.exit(1)
    }

  }
}

