
#include "postgres.h"
#include "pg_timer.h"

int mod_value;

TimerManager timer_manager;

void init_timermanager(void)
{
	timer_manager.measured_count = 0;
	timer_manager.flag = false;
}

void stamp_start(void)
{
	if (unlikely(timer_manager.flag))
	{
		elog(WARNING, "calling stamp_start twice");
		return;
	}

	if (unlikely(timer_manager.measured_count == MAX_TIMER_COUNT))
	{
		elog(WARNING, "timer blocks are fulled");
		return;
	}

	clock_gettime(CLOCK_MONOTONIC, &(timer_manager.timer_blocks[timer_manager.measured_count].start));
	timer_manager.flag = true;
}

void stamp_end(void)
{
	if (unlikely(!timer_manager.flag))
	{
		elog(WARNING, "calling stamp_end twice");
		return;
	}

	clock_gettime(CLOCK_MONOTONIC, &(timer_manager.timer_blocks[timer_manager.measured_count].end));
	timer_manager.flag = false;
	timer_manager.measured_count++;
}

void print_timermanager(char *trx_name)
{
	struct timespec start;
	struct timespec end;

	uint64 start_time;
	uint64 end_time;

	uint64 elapsed_time;

	fprintf(stderr, "------------------------------------------\n");

	for (int i = 0; i < timer_manager.measured_count; i++)
	{
		start = timer_manager.timer_blocks[i].start;
		end = timer_manager.timer_blocks[i].end;

		start_time = start.tv_sec * 1000000000 + start.tv_nsec;
		end_time = end.tv_sec * 1000000000 + end.tv_nsec;

		elapsed_time = end_time - start_time;

		fprintf(stderr, "[TIME_LOG] %s %d %lu\n", trx_name, i, elapsed_time);
	}

	fprintf(stderr, "------------------------------------------\n");
}