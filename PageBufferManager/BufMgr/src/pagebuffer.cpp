/**
 * @author Additional information regarding authorship and licensing are available in Supplementary.txt
 **/

#include <iostream>
#include <memory>

#include "pagebuffer.h"
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
			delete &bufferStatTable[i];
		}
		delete[] bufferStatTable;
		delete[] pageBufferPool;
		// END of your solution -- do not remove this comment
	}

	void PageBufferManager::readPage(File *file, const PageId pageNumber, Page *&page)
	{
		// BEGINNING of your solution -- do not remove this comment

		// END of your solution -- do not remove this comment
	}

	void PageBufferManager::allocatePage(File *file, PageId &pageNumber, Page *&page)
	{
		// BEGINNING of your solution -- do not remove this comment

		// END of your solution -- do not remove this comment
	}

	void PageBufferManager::unPinPage(File *file, const PageId pageNumber, const bool dirty)
	{
		// BEGINNING of your solution -- do not remove this comment

		// END of your solution -- do not remove this comment
	}

	void PageBufferManager::disposePage(File *file, const PageId pageNumber)
	{
		// BEGINNING of your solution -- do not remove this comment

		// END of your solution -- do not remove this comment
	}

	void PageBufferManager::advanceClock()
	{
		// BEGINNING of your solution -- do not remove this comment

		// END of your solution -- do not remove this comment
	}

	void PageBufferManager::allocateBuffer(FrameId &frame)
	{
		// BEGINNING of your solution -- do not remove this comment

		// END of your solution -- do not remove this comment
	}

	void PageBufferManager::flushFile(const File *file)
	{
		// BEGINNING of your solution -- do not remove this comment

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
