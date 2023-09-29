"""
  This is the main entry point of partitioning
  This complete the whole process of partitioning
  From creating the partition, migrating the data, and combining the partition

  This is not encourgage to be used in production yet as it still perform slower
"""
import time

from partitioning.combine import CombineMigration
from partitioning.microbatching import MicrobatchMigration
from partitioning.partition import Partition
from partitioning.partition_complete import CompletionhMigration

if __name__ == "__main__":
  start = time.time()
  runner = Partition()
  runner.main()

  microbatch = MicrobatchMigration()
  microbatch.main()

  completion = CompletionhMigration()
  completion.main()

  combine = CombineMigration()
  combine.main()
  end = time.time()
  print(end - start)
