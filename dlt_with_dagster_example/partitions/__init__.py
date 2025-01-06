from dagster import (
    DailyPartitionsDefinition,
    MonthlyPartitionsDefinition,
    WeeklyPartitionsDefinition,
    StaticPartitionsDefinition,
)


my_partitions = StaticPartitionsDefinition(['partition1', 'partition2', 'partition3', 'partition4'])
