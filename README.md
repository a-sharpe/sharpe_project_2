Names: Alexander Sharpe and Cristobal Benavides 

The local results for Parts 1 and 2 were computed using the Apple M2 Pro chip. For the GCP results, we limited the worker nodes to 100 GB of disk space due to project quota errors when more than 100 GBs were used.

# Part 1: exact_F2 Function

- Local Results:
  - Time Elapsed: 23 sec
  - Estimate: 8567966130
    
- GCP Results (1 driver, 4 machines w/ 2x N1 cores):
  - Time Elapsed: 74 sec
  - Estimate: 8567966130

# Part 2: Tug_of_War Function

- Local Results:
  - Time Elapsed: 907 sec 
  - Estimate: 9397081282
    
- GCP Results (1 driver, 4 machines w/ 2x N1 cores):
  - Time Elapsed: 203 sec 
  - Estimate: 8342440577

# Part 3: BJKST Function

- Local Results:
  - Time Elapsed: 1000s
  - Estimate: 4.0647854525584167
    
- GCP Results (1 driver, 4 machines w/ 2x N1 cores):
  - Time Elapsed: 338s
  - Estimate: 4.349122446706388E7 with 20000 trial

# Part 5: Comparison

We then compared the respective algorithms using the GCP results. our BJKST sketch estimate of 43.49 million is inaccurate compared to the true distinct count of 7.41 million — it overshoots by nearly 487%, outside of the ±20%  range. This deviation indicates that the sketch’s bucket size (width) is far too small to capture enough of the low‑hash values or the implemenation was not done correctly.

| Algorithm  | Run Time (sec) | Results |
| --- | --- | --- |
| exact_F0   |    28   |  7406649| 
| BJKST      |   338  |  4.349122446706388E7   |

Due to the variance induced by the Tug of War Algorithm, the answer does not match the exact F2 exactly (97% accurate). Additionally, due to the the computational overhead of computing with 10x3 sketches, the time it takes does not actually improve over the brute force, exact method. However, if we reduce the number of sketches, Tug of War will be faster, albeit less accurate.

| Algorithm  | Run Time (sec) | Results |
| --- | --- | --- |
| exact_F2   |   74       |   8567966130   | 
| Tug_of_War |  203     | 8342440577  |

