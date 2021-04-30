/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <stack>
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

BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType)
{
	//Get the index name
	std::ostringstream idxStr;
	idxStr << relationName << '.' << attrByteOffset;
	outIndexName = idxStr.str();

	//Set up members
	this->scanExecuting = false;
  	this->bufMgr = bufMgrIn;
	this->attributeType = attrType;
	this->attrByteOffset = attrByteOffset;
  	this->headerPageNum = 1;
	this->leafOccupancy = 0;
	this->nodeOccupancy = 0;

	IndexMetaInfo* meta;
	Page *headerPage, *rootPage;
	//flipped the try to use FileNotFoundException instead of FileExistsException
	try {
		this->file = new BlobFile(outIndexName, false);
		this->headerPageNum = this->file->getFirstPageNo();
		this->bufMgr->readPage(file, headerPageNum, headerPage);
		meta = (IndexMetaInfo*) headerPage;
    	this->rootPageNum = meta->rootPageNo;
		try {
			this->bufMgr->unPinPage(file, headerPageNum, false);
		} catch(PageNotPinnedException& e){ }
	} catch (FileNotFoundException& e) {
		this->file = new BlobFile(outIndexName, true);
	}
 
	this->bufMgr->allocPage(file, this->headerPageNum, headerPage);
	this->bufMgr->allocPage(file, this->rootPageNum, rootPage);
	meta = (IndexMetaInfo*) headerPage;
    meta->attrByteOffset = attrByteOffset;
	meta->attrType = attrType;
	meta->rootPageNo = this->rootPageNum;
	strcpy( meta->relationName, relationName.c_str() );
	NonLeafNodeInt* root = (NonLeafNodeInt*) rootPage;
	root->level = 1;

	try {
		this->bufMgr->unPinPage(file, headerPageNum, true);
		this->bufMgr->unPinPage(file, rootPageNum, true);
	} catch(PageNotPinnedException& e){ }

	//This part could be switched up a bit
	try {
		FileScan fileScan(relationName, bufMgr);
		RecordId rid = {};
		int count = 0;
		while (true) {
			fileScan.scanNext(rid);
			std::string record = fileScan.getRecord();
			insertEntry( (int*) record.c_str() + attrByteOffset, rid);
			count++;
		}
	}catch (EndOfFileException e) {}
}

BTreeIndex::~BTreeIndex()
{
  this->scanExecuting = false;
  this->bufMgr -> flushFile(file);
  delete file;
}

//TODO
void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
	Page* currentPage;
	bufMgr->readPage(file, rootPageNum, currentPage);
  	LeafNodeInt* leafNode;
	NonLeafNodeInt* currentNode = (NonLeafNodeInt*) currentPage;
	int currentKey = *((int *) key);
  	std::stack<PageId> stack;
  	stack.push(rootPageNum);
  	int id;
  
	while (true) {
 
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
			try {
	  			this->bufMgr->unPinPage(this->file, lastPageId, true);
			} catch(PageNotPinnedException& e){ }
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
   
	}

	if (!insertKeyLeafNode(leafNode, currentKey, rid)) {
		PageId newPageId = splitLeafNode(leafNode, currentKey, rid);
		try{
			this->bufMgr->unPinPage(this->file, stack.top(), true);
		} catch(PageNotPinnedException& e){ }
		stack.pop();
		PageId currentPageId = stack.top();
		bufMgr->readPage(file, currentPageId, currentPage);
		this->bufMgr->unPinPage(this->file, currentPageId, true);
		currentNode = (NonLeafNodeInt*) currentPage;

		while (!insertKeyInternalNode(currentNode, currentKey, newPageId)) {
			newPageId = splitInternalNode(currentNode, currentKey, newPageId);
			this->bufMgr->unPinPage(this->file, currentPageId, true);
			stack.pop();
			if (stack.empty()){
				break;
      }
			currentPageId = stack.top();
			bufMgr->readPage(file, currentPageId, currentPage);
			currentNode = (NonLeafNodeInt*) currentPage;
		}
		this->bufMgr->unPinPage(this->file, currentPageId, true);

		if (stack.empty()) {
			Page* rootPage;
			PageId rootPageId;
			try {
				this->bufMgr->allocPage(file, rootPageId, rootPage);
			} catch(PageNotPinnedException& e){ }
			NonLeafNodeInt* root = (NonLeafNodeInt*) rootPage;
			root->level = 0;
			root->keyArray[0] = currentKey;
			root->pageNoArray[0] = currentPageId;
			root->pageNoArray[1] = newPageId;
			rootPageNum = rootPageId;
			try {
				this->bufMgr->unPinPage(this->file, newPageId, true);
				this->bufMgr->unPinPage(this->file, rootPageId, true);
			} catch(PageNotPinnedException& e){ }
		}
	}
 
	while (!stack.empty()) {
		try {
			this->bufMgr->unPinPage(this->file, stack.top(), true);
		} catch(PageNotPinnedException& e){ }
		stack.pop();
	}
}

void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{
  if (scanExecuting) {
		endScan();
  }
  lowValInt = *(int*)lowValParm;
  highValInt = *(int*)highValParm;
  scanExecuting = true;
  if(!((lowOpParm == GT or lowOpParm == GTE) and (highOpParm == LT or highOpParm == LTE))) {
    throw BadOpcodesException();
  }
  if(lowValInt > highValInt) {
    throw BadScanrangeException();
  }
  this->lowOp = lowOpParm;
  this->highOp = highOpParm;
  this->nextEntry = getNextEntry(rootPageNum);
}

void BTreeIndex::scanNext(RecordId& outRid) 
{
	if (!scanExecuting) throw ScanNotInitializedException();
  
	LeafNodeInt* currentNode = (LeafNodeInt*) currentPageData;

	while (1) {
		if (this->nextEntry == INTARRAYLEAFSIZE) {
      		this->nextEntry = 0;
			try {
				this->bufMgr->unPinPage(this->file, this->currentPageNum, false);
			} catch(PageNotPinnedException& e){ }
			this->currentPageNum = currentNode->rightSibPageNo;
			this->bufMgr->readPage(this->file, this->currentPageNum, this->currentPageData);
			if (!currentNode->rightSibPageNo/* == 0*/) {
				throw IndexScanCompletedException(); 
			}
      		currentNode = (LeafNodeInt*) this->currentPageData;
		}
  
		if (currentNode->ridArray[this->nextEntry].page_number == 0) {
			this->nextEntry = INTARRAYLEAFSIZE;
			continue;
		}
   
    	if (highOp == LT && currentNode->keyArray[this->nextEntry] >= highValInt) {
		  throw IndexScanCompletedException();
		} else if (currentNode->keyArray[this->nextEntry] > highValInt) {
		  throw IndexScanCompletedException();
    	}	
    
		if (lowOp == GT && currentNode->keyArray[this->nextEntry] <=lowValInt) {
		  this->nextEntry++;
		  continue;       
		} else if (currentNode->keyArray[this->nextEntry] < lowValInt) {
		  this->nextEntry++;
    	}
		break;
	}
	// this->nextEntry++;
	outRid = currentNode->ridArray[this->nextEntry];
}

void BTreeIndex::endScan() 
{
	if (!this->scanExecuting){
		throw ScanNotInitializedException();
  	}
	this->scanExecuting = false;
	try {
		this->bufMgr->unPinPage(this->file, this->currentPageNum, false);
	} catch(PageNotPinnedException& e){ }
}



//--Start of helper functions----------------------------------------------------

// These need to be changed still

//TODO
int BTreeIndex::getNextEntry(PageId pageNum) {
	this->currentPageNum = pageNum;
	this->bufMgr->readPage(this->file, this->currentPageNum, this->currentPageData);
	NonLeafNodeInt* currentNode = (NonLeafNodeInt*) this->currentPageData;
	int i = 0;
	for (; currentNode->pageNoArray[i+1] != 0 && i < INTARRAYNONLEAFSIZE && currentNode->keyArray[i] <= lowValInt; i++);

	if (currentNode->level == 1) {
		try {
			this->bufMgr->unPinPage(this->file, this->currentPageNum, false);
		} catch(PageNotPinnedException& e){ }
		this->currentPageNum = currentNode->pageNoArray[i];
		bufMgr->readPage(this->file, this->currentPageNum, this->currentPageData);
		LeafNodeInt* currentNode = (LeafNodeInt*) this->currentPageData;

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

	try {
		this->bufMgr->unPinPage(this->file, this->currentPageNum, false);
	} catch(PageNotPinnedException& e){ }
	return getNextEntry(currentNode->pageNoArray[i]);
}

//TODO
bool BTreeIndex::insertKeyLeafNode(LeafNodeInt *node, int key, const RecordId rid) {
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

//TODO
bool BTreeIndex::insertKeyInternalNode(NonLeafNodeInt* node, int key, PageId pageId) {
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

//TODO
PageId BTreeIndex::splitLeafNode(LeafNodeInt *lastNode, int& key, const RecordId rid) {
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
		insertKeyLeafNode(nextNode, key, rid);
	}else{
		insertKeyLeafNode(lastNode, key, rid);
  }
	key = nextNode->keyArray[0];
	try {
		this->bufMgr->unPinPage(this->file, nextPageId, true);
	} catch(PageNotPinnedException& e){ }
	return nextPageId;
}

//TODO
PageId BTreeIndex::splitInternalNode(NonLeafNodeInt* node, int &key, const PageId pageId) {
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
	try {
		this->bufMgr->unPinPage(file, newPageId, true);
	} catch(PageNotPinnedException& e){ }
	return newPageId;
	}
}