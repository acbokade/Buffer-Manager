/**
 * @author Additional information regarding authorship and licensing are available in Supplementary.txt
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
			hashTable->lookup(file, pageNumber, frameNo);
			bufferStatTable[frameNo].refbit = true;
			bufferStatTable[frameNo].pinCnt += 1;
			page = &pageBufferPool[frameNo];
		}
		catch (HashNotFoundException hashNotFoundException)
		{
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
		allocateBuffer(frameNo);
		page = &pageBufferPool[frameNo];
		*page = file->allocatePage();
		pageNumber = page->page_number();
		bufferStatTable[frameNo].Set(file, pageNumber);
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
				throw PageNotPinnedException(file->filename(), pageNumber, frameNo);
			}
			bufferStatTable[frameNo].pinCnt -= 1;
			if (dirty)
			{
				bufferStatTable[frameNo].dirty = true;
			}
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
			bufferStatTable[frameNo].Clear();
			hashTable->remove(file, pageNumber);
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
		clockHand = (clockHand + 1) % numBufs;
		// END of your solution -- do not remove this comment
	}

	void PageBufferManager::allocateBuffer(FrameId &frame)
	{
		// BEGINNING of your solution -- do not remove this comment
		uint32_t nPinnedPages = 0;
		while (true)
		{
			// For the first time, allocating buffer when isValid is false
			if (!bufferStatTable[clockHand].valid)
			{
				// allocated this frame
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
					// write to disk
					File *file = bufferStatTable[clockHand].file;
					file->writePage(pageBufferPool[clockHand]);
				}
				// Check if the frame has valid page in the hash table
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
			if (nPinnedPages == numBufs)
			{
				throw BufferExceededException();
			}
		}
		// END of your solution -- do not remove this comment
	}

	void PageBufferManager::flushFile(const File *file)
	{
		File file_copy = *file;
		// BEGINNING of your solution -- do not remove this comment
		for (FileIterator iter = file_copy.begin();
			 iter != file_copy.end();
			 ++iter)
		{
			PageId pageNo = (*iter).page_number();
			FrameId frameNo;
			hashTable->lookup(file, pageNo, frameNo);
			if (bufferStatTable[frameNo].pinCnt > 0)
			{
				throw PagePinnedException(file_copy.filename(), pageNo, frameNo);
			}
			if (!bufferStatTable[frameNo].valid)
			{
				throw BadBufferException(frameNo, bufferStatTable[frameNo].dirty, bufferStatTable[frameNo].valid, bufferStatTable[frameNo].refbit);
			}
			if (bufferStatTable[frameNo].dirty)
			{
				bufferStatTable[frameNo].file->writePage(pageBufferPool[frameNo]);
				bufferStatTable[frameNo].dirty = false;
			}
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

}
