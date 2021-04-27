/***
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
    // Add your code below. Please do not remove this line.

	//Construct index file name
	//Check if this index file exists
	//if it does, open it
	//if it doesn't create new

	std::ostringstream idxStr;
	idxStr << relationName << '.' << attrByteOffset;
	std::string indexName = idxStr.str(); //indexName is the name of the index file
	outIndexName = indexName; //sets the name of the file?

	bool newFile = true;
	if (BlobFile::exists(outIndexName)) { //check if this file already exists
		newFile = false;
	} 
	this->file = new BlobFile(outIndexName, newFile); //open the file -- unsure about this 
	this->bufMgr = bufMgrIn;
	this->attrByteOffset = attrByteOffset;
	this->attributeType = attrType;

	//Other members that I'm not sure what to do with yet
	//headerPageNum?
	//rootPageNum?
	//leafOccupancy?
	//nodeOccupancy?
}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
    // Add your code below. Please do not remove this line.
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
    // Add your code below. Please do not remove this line.
}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{
    // Add your code below. Please do not remove this line.

	this->scanExecuting = true;

	//Setup scanning variables

	//Check for proper low operator
	if (lowOpParm == GT || lowOpParm == GTE) {
		this->lowOp = lowOpParm;
	} else {
		throw BadOpcodesException();
	}

	//Check for proper high operator
	if (highOpParm == LT || lowOpParm == LTE) {
		this->highOp = highOpParm;	
	} else {
		throw BadOpcodesException();
	}

	//Set high/low values
	if (this->attributeType == INTEGER) {
		this->lowValInt = *lowValParm;
		this->highValInt = *highValParm;
	} else if (this->attributeType == DOUBLE) {
		this->lowValDouble = *lowValParm;
		this->highValDouble = *highValParm;
	} else if (this->attributeType == STRING) {
		this->lowValString = *lowValString;
		this->highValString = *highValString;
	} 

	//Other scanning members im not sure what to do with yet
	//nextEntry?
	//currentPageNum?
	//currentPageData?

	//End scanning variables setup

	//Begin scan
}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

void BTreeIndex::scanNext(RecordId& outRid) 
{
    // Add your code below. Please do not remove this line.
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
void BTreeIndex::endScan() 
{
    // Add your code below. Please do not remove this line.

	this->scanExecuting = false;
}

}
