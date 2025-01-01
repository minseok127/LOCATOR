/*-------------------------------------------------------------------------
 *
 * locator_md.h
 *    locator Magnetic Disk (used in traditional buffer pool)
 *
 *
 * src/include/locator/locator_md.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef LOCATOR_MD_H
#define LOCATOR_MD_H

#include <liburing.h>

#include "pg_config.h"
#include "access/xlogutils.h"
#include "storage/bufmgr.h"
#include "storage/fd.h"
#include "storage/relfilenode.h"

#include "locator/locator.h"

#define LocatorLseekBlocks(fd) \
	((BlockNumber) (lseek(fd, 0, SEEK_END) / BLCKSZ))

extern BlockNumber
LocatorNblocks(RelFileNode node, LocatorPartLevel partLvl,
			   LocatorPartNumber partNum, LocatorPartGenNo partGen);
extern int
LocatorCreatePartitionFile(RelFileNode node, LocatorPartLevel partLvl,
						   LocatorPartNumber partNum, LocatorPartGenNo partGen,
						   int flags);
extern int
LocatorOpenPartitionFile(RelFileNode node, LocatorPartLevel partLvl,
			  			 LocatorPartNumber partNum, LocatorPartGenNo partGen,
						 int flags);
extern void
LocatorExtend(RelFileNode node, LocatorPartLevel partLvl,
			  LocatorPartNumber partNum, LocatorPartGenNo partGen,
			  BlockNumber blockNum, char *buffer);
extern bool
LocatorWrite(RelFileNode node, LocatorPartLevel partLvl,
			 LocatorPartNumber partNum, LocatorPartGenNo partGen,
			 BlockNumber blockNum, char *buffer);
extern int
LocatorWritePartialColumnZone(int fd, BlockNumber blockNum,
							   char *buffer, int nblocks);
extern int
LocatorWriteBatch(int fd, BlockNumber blockNum, struct iovec *iovs,
				  int64_t iovcnt);
extern void
LocatorWriteback(RelFileNode node, LocatorPartLevel partLvl,
				 LocatorPartNumber partNum, LocatorPartGenNo partGen,
				 BlockNumber blockNum, BlockNumber nblocks);
extern void
LocatorRead(RelFileNode node, LocatorPartLevel partLvl,
			LocatorPartNumber partNum, LocatorPartGenNo partGen,
			BlockNumber blockNum, char *buffer);
extern void
LocatorPrepareRequest(struct io_uring* ring, int fd, struct iovec *iovs, 
					  BlockNumber blockNum, int iovcnt, void *io_info,
					  bool is_read);
extern int
LocatorSubmitRequest(struct io_uring* ring);

extern void
LocatorMempartExtend(RelFileNode node, LocatorPartLevel partLvl,
					 LocatorPartNumber partNum, LocatorPartGenNo partGen,
					 BlockNumber blockNum, char *buffer);

extern bool
LocatorMempartWrite(RelFileNode node, LocatorPartLevel partLvl,
					LocatorPartNumber partNum, LocatorPartGenNo partGen,
					char *buffer, int nblock);

extern void
LocatorMempartRead(RelFileNode node, LocatorPartLevel partLvl,
				   LocatorPartNumber partNum, LocatorPartGenNo partGen,
				   char *buffer);
#endif /* LOCATOR_MD_H */
