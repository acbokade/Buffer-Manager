/**
 * @author Ajinkya Bokade (A59019743) and Shanay Shah (A59010837)
 **/

#include <iostream>
#include <memory>

#include "pagebuffer.h"
#include "file_iterator.h"
#include "exceptions_header.h"

namespace badgerdb
{

	PageBufferManager::PageBufferManager(std::uint32_t buffers)
		: numBufs(buffers)
	{
		bufferStatTable = new BufferStatus[buffers];

		for (FrameId i = 0; i < buffers; i++)
		{
			bufferStatTable[i].frameNo = i;
			bufferStatTable[i].valid = false;
		}

		pageBufferPool = new Page[buffers];

		int htsize = ((((int)(buffers * 1.2)) * 2) / 2) + 1;
		hashTable = new BufHashTbl(htsize); // allocate the buffer hash table

		clockHand = buffers - 1;
	}

	PageBufferManager::~PageBufferManager()
	{
		// BEGINNING of your solution -- do not remove this comment
		for (FrameId i = 0; i < numBufs; i++)
		{
			// Flush out all dirty pages
			if (bufferStatTable[i].dirty)
			{
				File *file = bufferStatTable[i].file;
				file->writePage(pageBufferPool[i]);
			}
		}
		// Reclaim the heap memory
		delete[] bufferStatTable;
		delete[] pageBufferPool;
		delete hashTable;
		// END of your solution -- do not remove this comment
	}

	void PageBufferManager::readPage(File *file, const PageId pageNumber, Page *&page)
	{
		// BEGINNING of your solution -- do not remove this comment
		FrameId frameNo;
		try
		{
			// Check whether the page is already in buffer pool
			hashTable->lookup(file, pageNumber, frameNo);
			bufferStatTable[frameNo].refbit = true;
			bufferStatTable[frameNo].pinCnt += 1;
			page = &pageBufferPool[frameNo];
		}
		catch (HashNotFoundException hashNotFoundException)
		{
			// Allocate new buffer frame and read the page into the buffer pool.
			allocateBuffer(frameNo);
			pageBufferPool[frameNo] = file->readPage(pageNumber);
			page = &pageBufferPool[frameNo];
			hashTable->insert(file, pageNumber, frameNo);
			bufferStatTable[frameNo].Set(file, pageNumber);
		}
		// END of your solution -- do not remove this comment
	}

	void PageBufferManager::allocatePage(File *file, PageId &pageNumber, Page *&page)
	{
		// BEGINNING of your solution -- do not remove this comment
		FrameId frameNo;
		// Allocate a new buffer from available frames using clock algorithm
		allocateBuffer(frameNo);
		page = &pageBufferPool[frameNo];
		// Allocate page on the file
		*page = file->allocatePage();
		// Set page number of the newly allocated page
		pageNumber = page->page_number();
		// Set the entries in the buffer stat table
		bufferStatTable[frameNo].Set(file, pageNumber);
		// Insert the file, page and frame entry in the hash table
		hashTable->insert(file, pageNumber, frameNo);
		// END of your solution -- do not remove this comment
	}

	void PageBufferManager::unPinPage(File *file, const PageId pageNumber, const bool dirty)
	{
		// BEGINNING of your solution -- do not remove this comment
		FrameId frameNo;
		try
		{
			hashTable->lookup(file, pageNumber, frameNo);
			if (bufferStatTable[frameNo].pinCnt == 0)
			{
				// Throw page not pinned exception if pin count is 0
				throw PageNotPinnedException(file->filename(), pageNumber, frameNo);
			}
			if (dirty)
			{
				// Set dirty bit to true if dirty paramter is true
				bufferStatTable[frameNo].dirty = true;
			}
			bufferStatTable[frameNo].pinCnt -= 1;
		}
		catch (HashNotFoundException hashNotFoundException)
		{
			throw HashNotFoundException(file->filename(), pageNumber);
		}
		// END of your solution -- do not remove this comment
	}

	void PageBufferManager::disposePage(File *file, const PageId pageNumber)
	{
		// BEGINNING of your solution -- do not remove this comment
		FrameId frameNo;
		try
		{
			hashTable->lookup(file, pageNumber, frameNo);
			// Clear buffer state table entries
			bufferStatTable[frameNo].Clear();
			// Remove entry from the hash table
			hashTable->remove(file, pageNumber);
			// Delete the page from the file
			file->deletePage(pageNumber);
		}
		catch (HashNotFoundException hashNotFoundException)
		{
			throw HashNotFoundException(file->filename(), pageNumber);
		}
		// END of your solution -- do not remove this comment
	}

	void PageBufferManager::advanceClock()
	{
		// BEGINNING of your solution -- do not remove this comment
		// Advance clock to the next frame in the buffer pool.
		clockHand = (clockHand + 1) % numBufs;
		// END of your solution -- do not remove this comment
	}

	void PageBufferManager::allocateBuffer(FrameId &frame)
	{
		// BEGINNING of your solution -- do not remove this comment
		// Allocates a free frame using the clock algorithm
		// If necessary, writing a dirty page back to disk
		uint32_t nPinnedPages = 0;
		while (true)
		{
			// For the first time, allocating buffer when isValid is false
			if (!bufferStatTable[clockHand].valid)
			{
				frame = clockHand;
				bufferStatTable[clockHand].Clear();
				return;
			}
			else if (bufferStatTable[clockHand].pinCnt > 0)
			{
				nPinnedPages += 1;
				advanceClock();
			}
			else if (bufferStatTable[clockHand].refbit)
			{
				bufferStatTable[clockHand].refbit = false;
				advanceClock();
			}
			else
			{
				if (bufferStatTable[clockHand].dirty)
				{
					// Write to disk if page is dirty
					File *file = bufferStatTable[clockHand].file;
					file->writePage(pageBufferPool[clockHand]);
				}
				// Remove entry from hash table if buffer frame has valid page in it
				if (bufferStatTable[clockHand].valid)
				{
					File *file = bufferStatTable[clockHand].file;
					PageId pageNo = bufferStatTable[clockHand].pageNo;
					hashTable->remove(file, pageNo);
				}
				bufferStatTable[clockHand].Clear();
				frame = clockHand;
				return;
			}
			// All pages are pinned, then throw buffer exceeded exception
			if (nPinnedPages == numBufs)
			{
				throw BufferExceededException();
			}
		}
		// END of your solution -- do not remove this comment
	}

	void PageBufferManager::flushFile(const File *file)
	{
		// BEGINNING of your solution -- do not remove this comment
		// Iterate through all the pages of the file
		for (FrameId i = 0; i < numBufs; i++)
		{
			FrameId frameNo = i;
			// If the page doesn't belong to the file, continue
			if (bufferStatTable[frameNo].file != file)
			{
				continue;
			}
			PageId pageNo = bufferStatTable[frameNo].pageNo;
			if (bufferStatTable[frameNo].pinCnt > 0)
			{
				File* file_ptr = bufferStatTable[frameNo].file;
				// Throw page pinned exception if the page is pinned
				throw PagePinnedException(file_ptr->filename(), pageNo, frameNo);
			}
			if (!bufferStatTable[frameNo].valid)
			{
				// Throw bad buffer exception if the frame is not valid
				throw BadBufferException(frameNo, bufferStatTable[frameNo].dirty, bufferStatTable[frameNo].valid, bufferStatTable[frameNo].refbit);
			}
			if (bufferStatTable[frameNo].dirty)
			{
				// Flush the page if its dirty
				bufferStatTable[frameNo].file->writePage(pageBufferPool[frameNo]);
				bufferStatTable[frameNo].dirty = false;
			}
			// Remove the entry from the hash table and clear the corresponding frame
			// in the buffer stat table, so that it can be set by the incoming request
			hashTable->remove(file, pageNo);
			bufferStatTable[frameNo].Clear();
		}
		// END of your solution -- do not remove this comment
	}

	void PageBufferManager::printSelf(void)
	{
		BufferStatus *tempPageBuffer;
		int validFrames = 0;

		for (std::uint32_t i = 0; i < numBufs; i++)
		{
			tempPageBuffer = &(bufferStatTable[i]);
			std::cout << "FrameNo:" << i << " ";
			tempPageBuffer->Print();

			if (tempPageBuffer->valid == true)
				validFrames++;
		}

		std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
	}

	int PageBufferManager::countDirtyPages(void)
	{
		// Counts all the dirty pages in the buffer pool which needs to be
		// flushed
		int dirtyPages = 0;
		BufferStatus *tempPageBuffer;
		for (std::uint32_t i = 0; i < numBufs; i++)
		{
			tempPageBuffer = &(bufferStatTable[i]);

			if (tempPageBuffer->dirty == true)
				dirtyPages++;
		}
		return dirtyPages;
	}

}
