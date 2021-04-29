/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"
#include "exceptions/file_exists_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/hash_not_found_exception.h"


//#define DEBUG

namespace badgerdb
{

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType)
{
  headerPageNum = 1;
	leafOccupancy = 0;
	nodeOccupancy = 0;
	scanExecuting = false;
 
	std::ostringstream idxStr;
	idxStr << relationName << '.' << attrByteOffset;
	outIndexName = idxStr.str();
 
  bufMgr = bufMgrIn;
	attributeType = attrType;
	this->attrByteOffset = attrByteOffset;

	IndexMetaInfo* meta;
	Page *headerPage, *rootPage;

	try {
		file = new BlobFile(outIndexName, true);
	}catch (FileExistsException& e){
		file = new BlobFile(outIndexName, false);
		headerPageNum = file->getFirstPageNo();
		bufMgr->readPage(file, headerPageNum, headerPage);
		meta = (IndexMetaInfo*) headerPage;
    rootPageNum = meta->rootPageNo;
		bufMgr->unPinPage(file, headerPageNum, false);
	}
 
	bufMgr->allocPage(file, headerPageNum, headerPage);
	bufMgr->allocPage(file, rootPageNum, rootPage);
	meta = (IndexMetaInfo*) headerPage;
  meta->attrByteOffset = attrByteOffset;
	meta->attrType = attrType;
	meta->rootPageNo = rootPageNum;
	strcpy( meta->relationName, relationName.c_str() );
	NonLeafNodeInt* root = (NonLeafNodeInt*) rootPage;
	root->level = 1;
 
	BufferUnPinPage(headerPageNum, true);
	BufferUnPinPage(rootPageNum, true);
	try {
		FileScan fileScan(relationName, bufMgr);
		RecordId rid = {};
		int count = 0;
		while (1) {
			fileScan.scanNext(rid);
			std::string record = fileScan.getRecord();
			insertEntry( (int*) record.c_str() + attrByteOffset, rid);
			count++;
		}
	}catch (EndOfFileException e) {}
}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
	scanExecuting = false;
  bufMgr -> flushFile(file);
  delete file;
  file = nullptr;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

const void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
	Page* currentPage;
	bufMgr->readPage(file, rootPageNum, currentPage);
  LeafNodeInt* leafNode;
	NonLeafNodeInt* currentNode = (NonLeafNodeInt*) currentPage;
	int currentKey = *((int *) key);
  std::stack<PageId> stack;
	stack.push(rootPageNum);
  int id;
  
	while (1) {
 
		for ( id = 0; id < INTARRAYNONLEAFSIZE && currentNode->pageNoArray[id + 1] != 0 &&  
          currentNode->keyArray[id] < currentKey; id++);

		if (id == 0 && currentNode->pageNoArray[0] == 0) {
			Page *lastPage, *nextPage;
			PageId lastPageId, nextPageId;
			bufMgr-> allocPage(file, lastPageId, lastPage);
			bufMgr-> allocPage(file, nextPageId, nextPage);
      
			LeafNodeInt* lastNode = (LeafNodeInt*) lastPage;
			LeafNodeInt* nextNode = (LeafNodeInt*) nextPage;
			leafNode = nextNode;
      currentNode->keyArray[0] = currentKey;
			currentNode->pageNoArray[0] = lastPageId;
			currentNode->pageNoArray[1] = nextPageId;
			lastNode->rightSibPageNo = nextPageId;
      
			BufferUnPinPage(lastPageId, true);
			stack.push(nextPageId);
		}

		bufMgr->readPage(file, currentNode->pageNoArray[id], currentPage);
		stack.push(currentNode->pageNoArray[id]);

		if (currentNode->level == 1) {
			leafNode = (LeafNodeInt*) currentPage;
			break;
		} else {
			currentNode = (NonLeafNodeInt*) currentPage;
		}
   
	} //end while

	if (!insertKeyIntoLeaf(leafNode, currentKey, rid)) {
		PageId newPageId = splitLeaf(leafNode, currentKey, rid);
		BufferUnPinPage(stack.top(), true);
		stack.pop();
		PageId currentPageId = stack.top();
		bufMgr->readPage(file, currentPageId, currentPage);
		BufferUnPinPage(currentPageId, true);
		currentNode = (NonLeafNodeInt*) currentPage;

		while (!insertKeyIntoNonLeaf(currentNode, currentKey, newPageId)) {
			newPageId = splitNonLeaf(currentNode, currentKey, newPageId);
			BufferUnPinPage(currentPageId, true);
			stack.pop();
			if (stack.empty()){
				break;
      }
			currentPageId = stack.top();
			bufMgr->readPage(file, currentPageId, currentPage);
			currentNode = (NonLeafNodeInt*) currentPage;
		}
		BufferUnPinPage(currentPageId, true);

		if (stack.empty()) {
			Page* rootPage;
			PageId rootPageId;
			bufMgr->allocPage(file, rootPageId, rootPage);
			NonLeafNodeInt* root = (NonLeafNodeInt*) rootPage;
			root->level = 0;
			root->keyArray[0] = currentKey;
			root->pageNoArray[0] = currentPageId;
			root->pageNoArray[1] = newPageId;
			rootPageNum = rootPageId;
			BufferUnPinPage(newPageId, true);
			BufferUnPinPage(rootPageId, true);
		}
	} //end big if
 
	while (!stack.empty()) {
		BufferUnPinPage(stack.top(), true);
		stack.pop();
	}
}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

const void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{
  if (scanExecuting) {
		endScan();
  }
  lowValInt = *(int *)lowValParm;
	highValInt = *(int *)highValParm;
  scanExecuting = true;
  if(!((lowOpParm == GT or lowOpParm == GTE) and (highOpParm == LT or highOpParm == LTE))) {
    throw BadOpcodesException();
  }
  if(lowValInt > highValInt) {
    throw BadScanrangeException();
  }
 	lowOp = lowOpParm;
	highOp = highOpParm;
	nextEntry = findNextEntry(rootPageNum);
}

// -----------------------------------------------------------------------------
// BTreeIndex::findNextEntry
// -----------------------------------------------------------------------------

const int BTreeIndex::findNextEntry(PageId pageNum) {
	currentPageNum = pageNum;
	bufMgr->readPage(file, currentPageNum, currentPageData);
	NonLeafNodeInt* currentNode = (NonLeafNodeInt*) currentPageData;
	int i = 0;
	for (; currentNode->pageNoArray[i+1] != 0 && i < INTARRAYNONLEAFSIZE && currentNode->keyArray[i] <= lowValInt; i++);

	if (currentNode->level == 1) {
		BufferUnPinPage(currentPageNum, false);
		currentPageNum = currentNode->pageNoArray[i];
		bufMgr->readPage(file, currentPageNum, currentPageData);
		LeafNodeInt* currentNode = (LeafNodeInt*) currentPageData;

		int left = 0;
    int right = INTARRAYLEAFSIZE - 1;
		
		int j = 0;
		for (j = left; j <= right; j++)
			if (lowOp == GT && currentNode->keyArray[j] > lowValInt) {
					return j;
			} else if (currentNode->keyArray[j] >= lowValInt) {
					return j;
			}
		return j;
	}

	BufferUnPinPage(currentPageNum, false);
	return findNextEntry(currentNode->pageNoArray[i]);
}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

const void BTreeIndex::scanNext(RecordId& outRid) 
{
	if (!scanExecuting) throw ScanNotInitializedException();
  
	LeafNodeInt* currentNode = (LeafNodeInt*) currentPageData;

	while (1) {
		if (nextEntry == INTARRAYLEAFSIZE) {
      nextEntry = 0;
			BufferUnPinPage(currentPageNum, false);
			currentPageNum = currentNode->rightSibPageNo;
			bufMgr->readPage(file, currentPageNum, currentPageData);
			if (currentNode->rightSibPageNo == 0) throw IndexScanCompletedException();
      
      currentNode = (LeafNodeInt*) currentPageData;
		}
  
		if (currentNode->ridArray[nextEntry].page_number == 0) {
			nextEntry = INTARRAYLEAFSIZE;
			continue;
		}
   
    if (highOp == LT && currentNode->keyArray[nextEntry] >= highValInt) {
		  throw IndexScanCompletedException();
		} else if (currentNode->keyArray[nextEntry] > highValInt) {
		  throw IndexScanCompletedException();
    }
    
		if (lowOp == GT && currentNode->keyArray[nextEntry] <=lowValInt) {
		  nextEntry++;
		  continue;       
		} else if (currentNode->keyArray[nextEntry] < lowValInt) {
		  nextEntry++;
    }
		break;
	}
	outRid = currentNode->ridArray[nextEntry];
	nextEntry++;
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------

const void BTreeIndex::endScan() 
{
	if (!scanExecuting){
		throw ScanNotInitializedException();
  }
	scanExecuting = false;
	BufferUnPinPage(currentPageNum, false);
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertKeyIntoLeaf
// -----------------------------------------------------------------------------

const bool BTreeIndex::insertKeyIntoLeaf(LeafNodeInt *node, int key, const RecordId rid) {
	if (node->ridArray[INTARRAYLEAFSIZE - 1].page_number != 0) {
		return false;
  }

	int i;
	for (i = 0; i < INTARRAYLEAFSIZE && node->ridArray[i].page_number != 0 && node->keyArray[i] < key; i++);

	int j;
	for (j = i; node->ridArray[j].page_number != 0; j++);

	for (int k = j; k > i; k--) {
		node->keyArray[k] = node->keyArray[k - 1];
		node->ridArray[k] = node->ridArray[k - 1];
	}
	node->keyArray[i] = key;
	node->ridArray[i] = rid;
	return true;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertKeyIntoNonLeaf
// -----------------------------------------------------------------------------

const bool BTreeIndex::insertKeyIntoNonLeaf(NonLeafNodeInt* node, int key, PageId pageId) {
	if (node->pageNoArray[INTARRAYNONLEAFSIZE] != 0)
		return false;

	int i;
	for (i = 0; i < INTARRAYNONLEAFSIZE && node->pageNoArray[i + 1] != 0 && node->keyArray[i] < key; i++);

	int j;
	for (j = i; node->pageNoArray[j + 1] != 0; j++);

	for (int k = j; k > i; k--) {
		node->keyArray[k] = node->keyArray[k - 1];
		node->pageNoArray[k + 1] = node->pageNoArray[k];
	}
	node->keyArray[i] = key;
	node->pageNoArray[i + 1] = pageId;
	return true;
}
// -----------------------------------------------------------------------------
// BTreeIndex::splitLeaf
// -----------------------------------------------------------------------------

const PageId BTreeIndex::splitLeaf(LeafNodeInt *lastNode, int& key, const RecordId rid) {
	Page* nextPage;
	PageId nextPageId;
	bufMgr->allocPage(file, nextPageId, nextPage);
	LeafNodeInt* nextNode = (LeafNodeInt*) nextPage;

	int center = (INTARRAYLEAFSIZE + 1) >> 1;
	for (int i = center; i < INTARRAYLEAFSIZE; i++) {
    nextNode->ridArray[i - center] = lastNode->ridArray[i];
		nextNode->keyArray[i - center] = lastNode->keyArray[i];
    lastNode->keyArray[i] = -1;
    lastNode->ridArray[i].page_number = 0;
    lastNode->ridArray[i].slot_number = Page::INVALID_SLOT;   
	} 
   
	nextNode->rightSibPageNo = lastNode->rightSibPageNo;
	lastNode->rightSibPageNo = nextPageId;

	if (key >= nextNode->keyArray[0]){
		insertKeyIntoLeaf(nextNode, key, rid);
	}else{
		insertKeyIntoLeaf(lastNode, key, rid);
  }
	key = nextNode->keyArray[0];
	BufferUnPinPage(nextPageId, true);
	return nextPageId;
}

// -----------------------------------------------------------------------------
// BTreeIndex::splitNonLeaf
// -----------------------------------------------------------------------------

const PageId BTreeIndex::splitNonLeaf(NonLeafNodeInt* node, int &key, const PageId pageId) {
	Page* newPage;
	PageId newPageId;
	bufMgr->allocPage(file, newPageId, newPage);
	NonLeafNodeInt* newNode = (NonLeafNodeInt*) newPage;
 
	int center = (INTARRAYNONLEAFSIZE + 1 + 1) >> 1;
	int lastKey = INT8_MIN;
	int keyTempArray[INTARRAYNONLEAFSIZE + 1];
	PageId pageNumber[INTARRAYNONLEAFSIZE + 2];
	pageNumber[0] = node->pageNoArray[0];

	int i, j;
	for (i = 0, j = 0; j < INTARRAYNONLEAFSIZE; i++) {
		if (lastKey <= key && key < node->keyArray[j]) {
			pageNumber[i] = pageId;
      keyTempArray[i] = key;
			lastKey = node->keyArray[j];
			continue;
		}
		lastKey = keyTempArray[i] = node->keyArray[j];
		pageNumber[i + 1] = node->pageNoArray[j + 1];
		j++;
	}

	if (i == j) {
		keyTempArray[i] = key;
		pageNumber[i + 1] = pageId;
	}

	node->pageNoArray[0] = pageNumber[0];

	for (int i = 0; i < center; i++) {
		node->keyArray[i] = keyTempArray[i];
		node->pageNoArray[i + 1] = pageNumber[i + 1];
	}

	newNode->pageNoArray[0] = pageNumber[center + 1];

	for (i = center; i <INTARRAYNONLEAFSIZE; i++) {
		newNode->keyArray[i - center] = keyTempArray[i + 1];
		newNode->pageNoArray[i - center + 1] = pageNumber[i + 2];
	}

	newNode->level = node->level;
	key = keyTempArray[center];
	BufferUnPinPage(newPageId, true);
	return newPageId;
}

const void BTreeIndex::BufferUnPinPage(const PageId pageNo, const bool dirty) {
	try {
		bufMgr->unPinPage(file, pageNo, dirty);
	} catch (PageNotPinnedException& e) {} catch (HashNotFoundException& e) {}
}

}