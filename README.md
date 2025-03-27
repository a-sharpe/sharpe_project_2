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

- Local Results (for some reason this took an extremely long time, more than 30 minutes, which I believe led to the discrepancy between these results and the GCP):
  - Time Elapsed: > 1800 sec 
  - Estimate: 21464343
    
- GCP Results (1 driver, 4 machines w/ 2x N1 cores):
  - Time Elapsed: 275 sec 
  - Estimate: 42790292

# Part 3: BJKST Function

- Local Results:
  - Time Elapsed:
  - Estimate:
    
- GCP Results (1 driver, 4 machines w/ 2x N1 cores):
  - Time Elapsed:
  - Estimate: 

# Part 4: Comparison

We then compared the respective algorithms using the GCP results. 

| Algorithm  | Run Time (sec) | Results |
| --- | --- | --- |
| exact_F0   |          |         | 
| BJKST      |          |         |

| Algorithm  | Run Time (sec) | Results |
| --- | --- | --- |
| exact_F2   |   74       |   8567966130   | 
| Tug_of_War |  275     | 42790292  |

