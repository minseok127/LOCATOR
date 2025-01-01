#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>

extern void sync(void);

int main(void)
{
    FILE* f;

    if (geteuid() != 0)
	{
        fprintf(stderr, "flush-cache: Not root\n");
        exit(EXIT_FAILURE);
    }

    printf("Flushing page cache, dentries and inodes...\n");

    // First: the traditional three sync calls. Perhaps not needed?
    // For security reasons, system("sync") is not a good idea.
    sync();
    sync();
    sync();

    f = fopen("/proc/sys/vm/drop_caches", "w");
    if (f == NULL)
	{
        fprintf(stderr, "flush-cache: Couldn't open /proc/sys/vm/drop_caches\n");
        exit(EXIT_FAILURE);
    }

    if (fprintf(f, "3\n") != 2)
	{
        fprintf(stderr, "flush-cache: Couldn't write 3 to /proc/sys/vm/drop_caches\n");
        exit(EXIT_FAILURE);
    }

    fclose(f);
    printf("Done flushing.\n");

    return EXIT_SUCCESS;
}

// https://stackoverflow.com/questions/13646925/allowing-a-non-root-user-to-drop-cache