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
		// Flush out all dirty pages
		std::cout << "Destructor of PageBufferManager called" << std::endl;
		for (FrameId i = 0; i < numBufs; i++)
		{
			if (bufferStatTable[i].dirty)
			{
				File *file = bufferStatTable[i].file;
				file->writePage(pageBufferPool[i]);
			}
		}
		delete[] bufferStatTable;
		delete[] pageBufferPool;
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
			*page = pageBufferPool[frameNo];
		}
		catch (HashNotFoundException hashNotFoundException)
		{
			allocateBuffer(frameNo);
			*page = file->readPage(pageNumber);
			hashTable->insert(file, pageNumber, frameNo);
			bufferStatTable[pageNumber].Set(file, pageNumber);
			// TODO: What about copying the page to pageBufferPool
			pageBufferPool[frameNo] = *page;
		}
		// END of your solution -- do not remove this comment
	}

	void PageBufferManager::allocatePage(File *file, PageId &pageNumber, Page *&page)
	{
		// BEGINNING of your solution -- do not remove this comment
		*page = file->allocatePage();
		FrameId frameNo;
		allocateBuffer(frameNo);
		hashTable->insert(file, pageNumber, frameNo);
		bufferStatTable[frameNo].Set(file, pageNumber);
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
				throw new PageNotPinnedException(__FILE__, pageNumber, bufferStatTable[pageNumber].frameNo);
			}
			bufferStatTable[frameNo].pinCnt -= 1;
			if (dirty)
			{
				bufferStatTable[frameNo].dirty = true;
			}
		}
		catch (HashNotFoundException hashNotFoundException)
		{
			throw new HashNotFoundException(__FILE__, pageNumber);
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
			delete &bufferStatTable[frameNo];
			hashTable->remove(file, pageNumber);
			// TODO: free pageBufferPool
			// pageBufferPool[frameNo] = nullptr;
			file->deletePage(pageNumber);
		}
		catch (HashNotFoundException hashNotFoundException)
		{
			throw new HashNotFoundException(__FILE__, pageNumber);
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
		for (FrameId i = 0; i < numBufs; i++)
		{
			if (bufferStatTable[i].pinCnt > 0)
			{
				advanceClock();
			}
			else
			{
				if (bufferStatTable[i].refbit)
				{
					bufferStatTable[i].refbit = false;
					advanceClock();
				}
				else
				{
					if (bufferStatTable[i].dirty)
					{
						// write to disk
						File *file = bufferStatTable[i].file;
						flushFile(file);
					}
					// Check if the frame has valid page in the hash table
					if (pageBufferPool[i].page_number() != Page::INVALID_NUMBER)
					{
						File *file = bufferStatTable[i].file;
						PageId pageNo = bufferStatTable[i].pageNo;
						hashTable->remove(file, pageNo);
					}
					frame = i;
				}
			}
		}
		throw new BufferExceededException();
		// END of your solution -- do not remove this comment
	}

	void PageBufferManager::flushFile(const File *file)
	{
		// BEGINNING of your solution -- do not remove this comment
		for (FileIterator iter = file->begin();
			 iter != file->end();
			 ++iter)
		{
			{
				PageId pageNo = (*iter).page_number();
				FrameId frameNo;
				hashTable->lookup(file, pageNo, frameNo);
				if (bufferStatTable[frameNo].pinCnt > 0)
				{
					throw new PagePinnedException(__FILE__, pageNo, frameNo);
				}
				if (!bufferStatTable[frameNo].valid)
				{
					throw new BadBufferException(frameNo, bufferStatTable[frameNo].dirty, bufferStatTable[frameNo].valid, bufferStatTable[frameNo].refbit);
				}
				if (bufferStatTable[frameNo].dirty)
				{
					file->writePage(pageBufferPool[frameNo]);
					bufferStatTable[frameNo].dirty = false;
				}
				hashTable->remove(file, pageNo);
				bufferStatTable[frameNo].Clear();
			}
			// END of your solution -- do not remove this comment
		}
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
