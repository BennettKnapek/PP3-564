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
	this->leafOccupancy = 0;
	this->nodeOccupancy = 0;

	IndexMetaInfo* metaData;
	Page *headerpg;
	Page *rootpg;

	try {
		this->file = new BlobFile(outIndexName, false);
		//File already exists, get the info 
		this->headerPageNum = this->file->getFirstPageNo();
		this->bufMgr->readPage(this->file, this->headerPageNum, headerpg);
	} catch (FileNotFoundException& e) {
		//File needs to be created
		this->file = new BlobFile(outIndexName, true);
		this->headerPageNum = 1;
		this->bufMgr->allocPage(file, this->headerPageNum, headerpg);
		this->bufMgr->allocPage(file, this->rootPageNum, rootpg);
	}

	metaData = (IndexMetaInfo*)headerpg;
    metaData->attrByteOffset = attrByteOffset;
	metaData->attrType = attrType;
	metaData->rootPageNo = this->rootPageNum;
	strcpy(metaData->relationName, relationName.c_str());
	this->rootPageNum = metaData->rootPageNo;
	NonLeafNodeInt* root = (NonLeafNodeInt*) rootpg;
	root->level = 1;

	//Unpin the header and root pages, no longer needed in pool
	try {
		this->bufMgr->unPinPage(file, headerPageNum, true);
		this->bufMgr->unPinPage(file, rootPageNum, true);
	} catch(PageNotPinnedException& e){ }

	//This part comes directly from main.cpp line 113-128
	FileScan fscan(relationName, bufMgr);
	try {
		RecordId rid;
		while (1) {
			fscan.scanNext(rid);
			std::string recordStr = fscan.getRecord();
			const char *record = recordStr.c_str();
			insertEntry((int*)record + attrByteOffset, rid);
		}
	} catch (EndOfFileException &e) {}
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
}

void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{
  if (scanExecuting) {
		endScan();
  }
  if (lowOpParm!=GT && lowOpParm != GTE && highOpParm != LT && highOpParm != LTE) {
    throw BadOpcodesException();
  }
  if(lowValInt > highValInt) {
    throw BadScanrangeException();
  }

  this->scanExecuting = true;
  this->lowValInt = *(int*)lowValParm;
  this->highValInt = *(int*)highValParm;
  this->lowOp = lowOpParm;
  this->highOp = highOpParm;
  this->nextEntry = getNextEntry(rootPageNum);
}

void BTreeIndex::scanNext(RecordId& outRid) 
{
}

void BTreeIndex::endScan() 
{
	if (!this->scanExecuting){
		throw ScanNotInitializedException();
  	}
	try {
		this->bufMgr->unPinPage(this->file, this->currentPageNum, false);
	} catch(PageNotPinnedException& e){ }
	this->scanExecuting = false;
}

//TODO
//Maybe convert this into an iterative function
int BTreeIndex::getNextEntry(PageId pageNum) {
	this->currentPageNum = pageNum;
	this->bufMgr->readPage(this->file, this->currentPageNum, this->currentPageData);
	NonLeafNodeInt* currentNode = (NonLeafNodeInt*) this->currentPageData;
	int i;
	for (i = 0; currentNode->pageNoArray[i+1] != 0 && i < INTARRAYNONLEAFSIZE && currentNode->keyArray[i] <= lowValInt; i++) {

		if (currentNode->level == 1) {
			try {
				this->bufMgr->unPinPage(this->file, this->currentPageNum, false);
			} catch(PageNotPinnedException& e){ }
			this->currentPageNum = currentNode->pageNoArray[i];
			bufMgr->readPage(this->file, this->currentPageNum, this->currentPageData);
			LeafNodeInt* currentNode = (LeafNodeInt*) this->currentPageData;

			int j;
			for (j = 0; j <= INTARRAYLEAFSIZE-1; j++) {
				if (this->lowOp == GT && currentNode->keyArray[j] > this->lowValInt || currentNode->keyArray[j] >= lowValInt) {
						return j;
				} 
			}
			return j;
		}
	}

	try {
		this->bufMgr->unPinPage(this->file, this->currentPageNum, false);
	} catch(PageNotPinnedException& e){ }
	return getNextEntry(currentNode->pageNoArray[i]);
}
}