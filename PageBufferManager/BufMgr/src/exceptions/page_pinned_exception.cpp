/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "page_pinned_exception.h"

#include <sstream>
#include <string>

namespace badgerdb
{

  PagePinnedException::PagePinnedException(const std::string &nameIn, PageId pageNoIn, FrameId frameNoIn)
      : BadgerDbException(""), name(nameIn), pageNo(pageNoIn), frameNo(frameNoIn)
  {
    std::stringstream ss;
    ss << "This page is already pinned. file:  " << name << "page: " << pageNo << "frame: " << frameNo;
    message_.assign(ss.str());
  }

}
