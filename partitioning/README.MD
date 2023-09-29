## This is the new method to partition a table

The ideal is simple, as we create a temporary table and copy all the data from existing to the new partition table.

1. Let say we have a table `Versions`
2. We will create a `Versions_temp` partition table including the `partition_key` (mention in config.yaml) inside the new primary key and default table
3. Use micro batch to insert data from `Versions` to range partition base on the date (i.e. `Versions_2020`)
4. Once all the data have been inserted, we will then attached all the range created to the temp table `Versions_temp`
   ```
    Versions_temp
    |__Versions_2020
    |__Versions_2021
    |__Versions_2022
    |__Versions_2023
   ```
5. Then we will count the `Versions` and `Versions_temp`
6. If both is the same, we will lock the table and perform the transition
7. First drop the `Versions` table
8. Rename `Versions_temp` to `Versions`
9.  Rename constrain to `versions_pkey`

By doing so, we can prevent the table from being lock until we completed all the migration of data, and lastly just dropping it.
Besides, this can reduce the needs of running vacuum to the table since we already have the newest table constantly running vacuum in the background

One of the drawback of running this is that it will be expensive for storage. This is because if u have a 500GB table, it will essentially create another 500GB table and until both side have equal data, it will remain the same until the end
