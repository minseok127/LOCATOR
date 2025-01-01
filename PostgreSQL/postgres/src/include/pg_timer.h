#ifndef PG_TIMER_H
#define PG_TIMER_H

#include "c.h"
#include <time.h>

#define MAX_TIMER_COUNT 8

typedef struct TimerBlock
{
	struct timespec start;
	struct timespec end;
} TimerBlock;

typedef struct TimerManager
{
	int measured_count;
	bool flag;
	TimerBlock timer_blocks[MAX_TIMER_COUNT];
} TimerManager;

extern PGDLLIMPORT int mod_value;

void init_timermanager(void);
void stamp_start(void);
void stamp_end(void);
void print_timermanager(char *proc_name);

#endif /* PG_TIMER_H */
