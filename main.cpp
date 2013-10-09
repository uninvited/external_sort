#include <boost/lockfree/queue.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/thread.hpp>
#include <iostream>
#include <fstream>
#include <vector>
#include <algorithm>
#include <string>
#include <stdlib.h>

#define TRACE(msg) std::cout << (msg) << endl;

using namespace std;

const string TMP_FILE_PREFIX = "__tmp.tmp";
const int MAX_SORTING_THREADS = 4;
const int MAX_MERGING_THREADS = 4;
#define MAX_FILES 200

// Shares of memory used by input streams and output stream when merging multiple files (in %)
const int IN_FILES_SHARE = 50;
const int OUT_FILE_SHARE = 100 - IN_FILES_SHARE;

static int fileIndex = 0;

int getNextFileIndex() {
	return fileIndex++;
}

// Generate name for a next temporary file
string getTmpFileName() {
	char buff[10];
	_itoa_s(getNextFileIndex(), buff, 10, 10);
	return TMP_FILE_PREFIX + string(buff);
}

// Align block size so that it is divisible by 4
inline size_t divisibleBy4(size_t val) {
	return val - (val % 4);
}

// Describes temporary file that is subject to merge with other file(s)
// Contains data block whose size is limited by maxSubBlockSize
// When current block is processed, new block is read from the file
class FileAbstraction {
public:
	FileAbstraction(string aFileName) :
	  fileName(aFileName),
      fileSize(0),
	  memoryLimit(0),
	  curSubBlock(NULL),
	  curSubBlockSize(0),
	  curSubBlockPos(0),
	  bytesRead(0)
	{}

	~FileAbstraction()	{
		delete[] curSubBlock;
		fd.close();
	}

	void open()	{
		fd.open(fileName.c_str(), std::ios::in | std::ios::binary);
		if (!fd.is_open())
		{
			TRACE ("Warning! Could not open the file " + fileName);
			return;
		}
		fd.seekg (0, fd.end);
		fileSize = (int)fd.tellg();
		fd.seekg (0, fd.beg);
		curSubBlock = new int[memoryLimit / sizeof(int)];
		readNextPortion();
	}

	void close() {
		if (fd.is_open())
			fd.close();
		delete[] curSubBlock;
	}

	// Removes the file from disk
	void destroy() {
		this->close();
		remove(fileName.c_str());
	}

	void readNextPortion() {
		if (isExhausted()) return;
		curSubBlockSize = std::min(memoryLimit, fileSize - bytesRead);
		fd.read ((char*)&curSubBlock[0], curSubBlockSize);
		bytesRead += curSubBlockSize;
		curSubBlockPos = 0;
	}

	string getFileName() const {
		return fileName;
	}

	int getCurElement()	const {
		return curSubBlock[curSubBlockPos];
	}

	int getCurElementAndAdvance() {
		int elem = curSubBlock[curSubBlockPos];
		advance();
		return elem;
	}

	size_t getFileSize() const {
		return fileSize;
	}
	
	void setMemoryLimit(size_t newLimit){
		memoryLimit = divisibleBy4(newLimit);
	}

	bool isExhausted() const {
		if (bytesRead >= fileSize && curSubBlockPos * sizeof(int) >= curSubBlockSize)
			return true;
		else
			return false;
	}
	
	void advance()	{
		if (!isExhausted())
		{
			curSubBlockPos++;
			if (curSubBlockPos * sizeof(int) >= curSubBlockSize)
				readNextPortion();
		}
	}

private:
	string fileName;
	ifstream fd;
	size_t fileSize;
	size_t bytesRead;
	int* curSubBlock;	
	size_t memoryLimit;

	size_t curSubBlockSize;
	size_t curSubBlockPos;
};


template <typename T>
class FileWrapper
{
public:
	FileWrapper(string fileName) : name(fileName) {}
	
	~FileWrapper(){
		if (f.is_open())
			f.close();
	}

	T* getFile() {
		return &f;
	}

	string getFileName() {
		return name;
	}

	void close() {
		f.close();
	}
	
protected:
	T f;
	string name;
};

class InputFile : public FileWrapper<ifstream>{
public:
	InputFile(string fileName) : FileWrapper(fileName) {
		name = fileName;
		f.open(fileName, std::ios::in | std::ios::binary);
		if (!f) throw string("Could not find the file");
	}

	int length() {
		f.seekg (0, f.end);
		int inFileLength = (int)f.tellg();
		f.seekg (0, f.beg);
		return inFileLength;
	}

	istream& read (char* s, streamsize n){
		return f.read(s, n);
	}
};

class OutputFile : public FileWrapper<ofstream>{
public:
	OutputFile(string fileName) : FileWrapper(fileName) {
		name = fileName;
		f.open(fileName, std::ios::out | std::ios::binary);
		if (!f) throw string("Could not find the file");
	}

	ostream& write (const char* s, streamsize n){
		return f.write(s, n);
	}
};

void sortingThread(vector<int>* v, string outFileName);

void mergeTwoFiles(FileAbstraction& file1,
	               FileAbstraction& file2,
				   string outFileName,
				   size_t memLimit);

void mergeThreeFiles(FileAbstraction& file1,
	                 FileAbstraction& file2,
					 FileAbstraction& file3,
					 string outFileName,
					 size_t memLimit);

void mergeFourFiles(FileAbstraction& file1,
	                FileAbstraction& file2,
					FileAbstraction& file3,
					FileAbstraction& file4,
					string outFileName, 
					size_t memLimit);

class WriteJob {
public:
	WriteJob() : dataLen(0), data(NULL), outFile(NULL), lastPortion(false) {}
	
	WriteJob(size_t len, OutputFile *f) : dataLen(len), outFile(f), lastPortion(false) {
		data = new int[dataLen];
	}

	~WriteJob() {
		delete[] data;
	}

	void setFile(OutputFile* f) {
		outFile = f;
	}

	void setIsLast(bool isLast) {
		lastPortion = isLast;
	}

	void setDataLen(size_t len) {
		dataLen = len;
	}

	size_t getDataLen() const {
		return dataLen;
	}

	int* getData() {
		return data;
	}

	bool isLast() const {
		return lastPortion;
	}

	OutputFile* getFile() const {
		return outFile;
	}

private:
	size_t dataLen;           // Length of data portion to be written
	int* data;                // Data portion to be written
	OutputFile *outFile;      // File to write the portion to
	bool lastPortion;         // indicates whether the file should be closed after write is done
	
};

boost::lockfree::queue<FileAbstraction*> fileQueue(MAX_FILES);

class SortingThreadInfo {
public:
	SortingThreadInfo() {}
	~SortingThreadInfo() {}

	vector<int> *getData() const {
		return data;
	}

	void start() {
		t = boost::thread(sortingThread, boost::ref(data), outFileName);
	}

	void alloc(int size) {		
		data = new vector<int>(size, 0);
	}

	void setFileName(string aFileName) {
		outFileName = aFileName;
	}

	void join()	{
		t.join();
		delete data;
	}
private:
	boost::thread t;    // Sorting thread
	vector<int> *data;  // Vector to sort
	string outFileName; // File name to write sorted data to
};

class MergingThreadInfo {
public:
	MergingThreadInfo(string fName) : fileName(fName) {}
	~MergingThreadInfo() {}
	
	void startMerge2(FileAbstraction *first, FileAbstraction* second, int memLimit) {
		t = boost::thread(mergeTwoFiles, boost::ref(*first), boost::ref(*second), fileName, memLimit);
	}

	void startMerge3(FileAbstraction *f1, FileAbstraction* f2, FileAbstraction* f3, int memLimit) {
		t = boost::thread(mergeThreeFiles, boost::ref(*f1), boost::ref(*f2), boost::ref(*f3), fileName, memLimit);
	}

	void startMerge4(FileAbstraction *f1, FileAbstraction* f2, FileAbstraction* f3, FileAbstraction* f4, int memLimit) {
		t = boost::thread(mergeFourFiles, boost::ref(*f1), boost::ref(*f2), boost::ref(*f3), boost::ref(*f4), fileName, memLimit);
	}

	void join()	{
		t.join();
		// TODO this should be moved to the merging function.
		fileQueue.push(new FileAbstraction(fileName));
	}
private:
	boost::thread t;
	string fileName;
};

void mergeFourFiles(FileAbstraction& file1,
	                  FileAbstraction& file2,
					  FileAbstraction& file3,
					  FileAbstraction& file4,
					  string outFileName, 
					  size_t memLimit)
{
	const int NUM_FILES = 4;
	size_t portion = divisibleBy4( (memLimit * OUT_FILE_SHARE / 100) / sizeof(int));
	size_t counter = 0;
	size_t fileSize = 0;	

	OutputFile* outFile = new OutputFile(outFileName.c_str());
	WriteJob *job = new WriteJob(portion, outFile);
	
	int* collector = job->getData();

	int inFileMemoryLimit = (memLimit * IN_FILES_SHARE / 100) / NUM_FILES;

	file1.setMemoryLimit(inFileMemoryLimit);
	file2.setMemoryLimit(inFileMemoryLimit);
	file3.setMemoryLimit(inFileMemoryLimit);
	file4.setMemoryLimit(inFileMemoryLimit);

	file1.open();
	file2.open();
	file3.open();
	file4.open();

	for (;;)
	{
		bool isLast = false;
		if ((!file1.isExhausted()) && (!file2.isExhausted()) && (!file3.isExhausted()) && (!file4.isExhausted())){
			int elem1 = file1.getCurElement();
			int elem2 = file2.getCurElement();
			int elem3 = file3.getCurElement();
			int elem4 = file4.getCurElement();

			if (elem1 < elem2 && elem1 < elem3 && elem1 < elem4)
				collector[counter++] = file1.getCurElementAndAdvance();
			else if (elem2 < elem3 && elem2 < elem4)
				collector[counter++] = file2.getCurElementAndAdvance();
			else if (elem3 < elem4)
				collector[counter++] = file3.getCurElementAndAdvance();
			else
				collector[counter++] = file4.getCurElementAndAdvance();
		}
		// Consider 3 files
		else if ((!file1.isExhausted()) && (!file2.isExhausted()) && (!file3.isExhausted()))
		{
			int elem1 = file1.getCurElement();
			int elem2 = file2.getCurElement();
			int elem3 = file3.getCurElement();

			if (elem1 < elem2 && elem1 < elem3)
				collector[counter++] = file1.getCurElementAndAdvance();
			else if (elem2 < elem3)
				collector[counter++] = file2.getCurElementAndAdvance();
			else
				collector[counter++] = file3.getCurElementAndAdvance();
		}
		else if ((!file1.isExhausted()) && (!file2.isExhausted()) && (!file4.isExhausted()))
		{
			int elem1 = file1.getCurElement();
			int elem2 = file2.getCurElement();
			int elem4 = file4.getCurElement();

			if (elem1 < elem2 && elem1 < elem4)
				collector[counter++] = file1.getCurElementAndAdvance();
			else if (elem2 < elem4)
				collector[counter++] = file2.getCurElementAndAdvance();
			else
				collector[counter++] = file4.getCurElementAndAdvance();
		}
		else if ((!file1.isExhausted()) && (!file3.isExhausted()) && (!file4.isExhausted()))
		{
			int elem1 = file1.getCurElement();
			int elem3 = file3.getCurElement();
			int elem4 = file4.getCurElement();

			if (elem1 < elem3 && elem1 < elem4)
				collector[counter++] = file1.getCurElementAndAdvance();
			else if (elem3 < elem4)
				collector[counter++] = file3.getCurElementAndAdvance();
			else
				collector[counter++] = file4.getCurElementAndAdvance();
		}
		else if ((!file2.isExhausted()) && (!file3.isExhausted()) && (!file4.isExhausted()))
		{
			int elem2 = file2.getCurElement();
			int elem3 = file3.getCurElement();
			int elem4 = file4.getCurElement();

			if (elem2 < elem3 && elem2 < elem4)
				collector[counter++] = file2.getCurElementAndAdvance();
			else if (elem3 < elem4)
				collector[counter++] = file3.getCurElementAndAdvance();
			else
				collector[counter++] = file4.getCurElementAndAdvance();
		}

		// Consider 2 files
		else if ((!file1.isExhausted()) && (!file2.isExhausted()))
		{
			int elem1 = file1.getCurElement();
			int elem2 = file2.getCurElement();
			collector[counter++] = elem1 < elem2 ? file1.getCurElementAndAdvance() : file2.getCurElementAndAdvance();
		}
		else if ((!file1.isExhausted()) && (!file3.isExhausted()))
		{
			int elem1 = file1.getCurElement();
			int elem3 = file3.getCurElement();
			collector[counter++] = elem1 < elem3 ? file1.getCurElementAndAdvance() : file3.getCurElementAndAdvance();
		}
		else if ((!file1.isExhausted()) && (!file4.isExhausted()))
		{
			int elem1 = file1.getCurElement();
			int elem4 = file4.getCurElement();
			collector[counter++] = elem1 < elem4 ? file1.getCurElementAndAdvance() : file4.getCurElementAndAdvance();
		}
		else if ((!file2.isExhausted()) && (!file3.isExhausted()))
		{
			int elem2 = file2.getCurElement();
			int elem3 = file3.getCurElement();
			collector[counter++] = elem2 < elem3 ? file2.getCurElementAndAdvance() : file3.getCurElementAndAdvance();
		}
		else if ((!file2.isExhausted()) && (!file4.isExhausted()))
		{
			int elem2 = file2.getCurElement();
			int elem4 = file4.getCurElement();
			collector[counter++] = elem2 < elem4 ? file2.getCurElementAndAdvance() : file4.getCurElementAndAdvance();
		}
		else if ((!file3.isExhausted()) && (!file4.isExhausted()))
		{
			int elem3 = file3.getCurElement();
			int elem4 = file4.getCurElement();
			collector[counter++] = elem3 < elem4 ? file3.getCurElementAndAdvance() : file4.getCurElementAndAdvance();
		}

		// Consider 1 file
		else if (!file1.isExhausted())
			collector[counter++] = file1.getCurElementAndAdvance();
		else if (!file2.isExhausted())
			collector[counter++] = file2.getCurElementAndAdvance();
		else if (!file3.isExhausted())
			collector[counter++] = file3.getCurElementAndAdvance();
		else if (!file4.isExhausted())
			collector[counter++] = file4.getCurElementAndAdvance();

		else // both files are exhausted
			isLast = true;

		// Check if it's time to write the portion to file
		if (counter >= portion || isLast)
		{
			job->setDataLen(counter);
			//job->setIsLast(isLast);
			
			job->getFile()->write((char*) job->getData(), job->getDataLen() * sizeof(int));
			delete job;
			fileSize += counter * sizeof(int);

			if (isLast)
				break;
			else
			{
				counter = 0;
				job = new WriteJob(portion, outFile);
				collector = job->getData();
			}
		}
	}

	file1.destroy();
	file2.destroy();
	file3.destroy();
	file4.destroy();

	outFile->close();
	delete outFile;
	//fileQueue.push(new FileAbstraction(outFileName));

	cout << "Resulting file " << outFileName << " is written, size: " << fileSize<< endl;
}



void mergeThreeFiles(FileAbstraction& file1,
	                  FileAbstraction& file2,
					  FileAbstraction& file3,
					  string outFileName, 
					  size_t memLimit)
{
	const int NUM_FILES = 3;
	size_t portion = divisibleBy4( (memLimit * OUT_FILE_SHARE / 100) / sizeof(int));
	size_t counter = 0;
	size_t fileSize = 0;	

	OutputFile* outFile = new OutputFile(outFileName.c_str());
	WriteJob *job = new WriteJob(portion, outFile);
	
	int* collector = job->getData();

	int inFileMemoryLimit = (memLimit * IN_FILES_SHARE / 100) / NUM_FILES;

	file1.setMemoryLimit(inFileMemoryLimit);
	file2.setMemoryLimit(inFileMemoryLimit);
	file3.setMemoryLimit(inFileMemoryLimit);

	file1.open();
	file2.open();
	file3.open();

	for (;;)
	{
		bool isLast = false;
		if ((!file1.isExhausted()) && (!file2.isExhausted()) && (!file3.isExhausted())){
			int elem1 = file1.getCurElement();
			int elem2 = file2.getCurElement();
			int elem3 = file3.getCurElement();
			
			if (elem1 < elem2 && elem1 < elem3)
				collector[counter++] = file1.getCurElementAndAdvance();
			else if (elem2 < elem3)
				collector[counter++] = file2.getCurElementAndAdvance();
			else
				collector[counter++] = file3.getCurElementAndAdvance();
		}
		else if ((!file1.isExhausted()) && (!file2.isExhausted()))
			collector[counter++] = file1.getCurElement() < file2.getCurElement()
									? file1.getCurElementAndAdvance()
									: file2.getCurElementAndAdvance();

		else if ((!file1.isExhausted()) && (!file3.isExhausted()))
			collector[counter++] = file1.getCurElement() < file3.getCurElement()
				                   ? file1.getCurElementAndAdvance()
			                       : file3.getCurElementAndAdvance();

		else if ((!file2.isExhausted()) && (!file3.isExhausted()))
			collector[counter++] = file2.getCurElement() < file3.getCurElement() 
                                   ? file2.getCurElementAndAdvance()
                                   : file3.getCurElementAndAdvance();

		else if (!file1.isExhausted())
			collector[counter++] = file1.getCurElementAndAdvance();
		else if (!file2.isExhausted())
			collector[counter++] = file2.getCurElementAndAdvance();
		else if (!file3.isExhausted())
			collector[counter++] = file3.getCurElementAndAdvance();

		else // both files are exhausted
			isLast = true;

		// Check if it's time to write the portion to file
		if (counter >= portion || isLast)
		{
			job->setDataLen(counter);
			//job->setIsLast(isLast);
			
			job->getFile()->write((char*) job->getData(), job->getDataLen() * sizeof(int));
			delete job;
			fileSize += counter * sizeof(int);

			if (isLast)
				break;
			else
			{
				counter = 0;
				job = new WriteJob(portion, outFile);
				collector = job->getData();
			}
		}
	}

	file1.destroy();
	file2.destroy();
	file3.destroy();

	outFile->close();
	delete outFile;
	//fileQueue.push(new FileAbstraction(outFileName));

	cout << "Resulting file " << outFileName << " is written, size: " << fileSize<< endl;
}

void mergeTwoFiles(FileAbstraction& file1, FileAbstraction& file2, string outFileName, size_t memLimit)
{
	const int NUM_FILES = 2;
	size_t portion = divisibleBy4( (memLimit * OUT_FILE_SHARE / 100) / sizeof(int));
	size_t counter = 0;
	size_t fileSize = 0;	

	OutputFile* outFile = new OutputFile(outFileName.c_str());
	WriteJob *job = new WriteJob(portion, outFile);
	int* collector = job->getData();

	int inFileMemoryLimit = (memLimit * IN_FILES_SHARE / 100) / NUM_FILES;

	file1.setMemoryLimit(inFileMemoryLimit);
	file2.setMemoryLimit(inFileMemoryLimit);

	file1.open();
	file2.open();

	for (;;)
	{
		bool isLast = false;
		if ((!file1.isExhausted()) && (!file2.isExhausted()))
			collector[counter++] = file1.getCurElement() < file2.getCurElement()
				                   ? file1.getCurElementAndAdvance() 
								   : file2.getCurElementAndAdvance();

		else if (!file1.isExhausted())
			collector[counter++] = file1.getCurElementAndAdvance();
		else if (!file2.isExhausted())
			collector[counter++] = file2.getCurElementAndAdvance();
		else // both files are exhausted
			isLast = true;

		// Check if it's time to write the portion to file
		if (counter >= portion || isLast)
		{
			job->setDataLen(counter);
			//job->setIsLast(isLast);

			job->getFile()->write((char*) job->getData(), job->getDataLen() * sizeof(int));
			delete job;

			fileSize += counter * sizeof(int);

			if (isLast)
				break;
			else
			{
				counter = 0;
				job = new WriteJob(portion, outFile);
				collector = job->getData();
			}
		}
	}

	file1.destroy();
	file2.destroy();

	outFile->close();
	delete outFile;
	//fileQueue.push(new FileAbstraction(outFileName));

	cout << "Resulting file " << outFileName << " is written, size: " << fileSize<< endl;
}

// Merge files in pairs: take first pair of files, merge them into the new file, add this file to the list.
// Repeat until there is only one file left.
void mergeAllFiles(string outFileName, size_t memLimit)
{
	list<MergingThreadInfo*> mthgroup;

	FileAbstraction* first;
	FileAbstraction* second;
	FileAbstraction* third;
	FileAbstraction* fourth;
	size_t filesToMerge = 4;

	while (true) {
		if (!fileQueue.pop(first))	{
			mthgroup.front()->join();
			mthgroup.pop_front();
			fileQueue.pop(first);
		}

		if (!fileQueue.pop(second))	{
			if (mthgroup.size() > 0) {
				mthgroup.front()->join();
				mthgroup.pop_front();
				fileQueue.pop(second);
				filesToMerge = 2;
			}
			else {
				remove(outFileName.c_str());
				rename(first->getFileName().c_str(), outFileName.c_str());
				break;
			}
		}
	
		if (!fileQueue.pop(third)) {
			if (mthgroup.size() > 0) {
				mthgroup.front()->join();
				mthgroup.pop_front();
				fileQueue.pop(third);
				filesToMerge = 3;
			}
		}

		if (filesToMerge > 2) {
			filesToMerge = 4;
			if(!fileQueue.pop(fourth)){
				if (mthgroup.size() > 0) {
					mthgroup.front()->join();
					mthgroup.pop_front();
					fileQueue.pop(fourth);
					filesToMerge = 4;
				}
				else
					filesToMerge = 3;
			}
		}
		
		// Make sure not more than MAX_MERGING_THREADS are running at a time
		// Join the oldest thread before starting a new one
		if (mthgroup.size() >= MAX_MERGING_THREADS)
		{
			mthgroup.front()->join();
			mthgroup.pop_front();
		}
		
		string mergedFileName(getTmpFileName());
		MergingThreadInfo *mt = new MergingThreadInfo(mergedFileName);

		if (filesToMerge == 4) {
			TRACE ("Start thread to merge 4 files: " + first->getFileName() + ", "
				                                       + second->getFileName() + ", "
													   + third->getFileName() + ", "
													   + fourth->getFileName());
			mt->startMerge4(first, second, third, fourth, memLimit / MAX_MERGING_THREADS);
		}
		else if (filesToMerge == 3) {
			TRACE ("Start thread to merge 3 files: " + first->getFileName() + ", "
				                                       + second->getFileName() + ", "
													   + third->getFileName());
			mt->startMerge3(first, second, third, memLimit / MAX_MERGING_THREADS);
		}
		else {
			TRACE ("Start thread to merge 3 files: " + first->getFileName() + ", " + second->getFileName());
			mt->startMerge2(first, second, memLimit / MAX_MERGING_THREADS);
		}
		mthgroup.push_back(mt);
	}
}

void sortingThread(vector<int>* v, string outFileName)
{
	std::sort(v->begin(), v->end());
	OutputFile tmpFile (outFileName.c_str());
	tmpFile.write((char*)&((*v)[0]), v->size() * sizeof(int));
	tmpFile.close();
}

// External sort includes 2 steps:
// Step 1: Reading the input by portions, sorting them and storing in temporary files.
// Step 2: Merging temporary files together using limited memory
int externalSort(const string& inFileName, const string& outFileName, size_t memLimit)
{
	try{
		InputFile inFile(inFileName.c_str());
		size_t inFileLength = inFile.length();
		memLimit -= (memLimit % sizeof(int));
		size_t threadMemLimit = divisibleBy4(memLimit / MAX_SORTING_THREADS);

		if (inFileLength % sizeof(int))
			throw string("Input file size must be divisible by 4");
		
		// There is enough memory to sort this file in one gulp. Simple sort goes
		if (inFileLength <= memLimit)
		{
			TRACE("Start simple sorting...");
			vector<int> v(inFileLength / sizeof(int), 0);
			inFile.read ((char*)&v[0], inFileLength);
			std::sort(v.begin(), v.end());
			OutputFile outFile(outFileName.c_str());
			outFile.write((char*)&v[0], inFileLength);
			outFile.close();
			return 0;
		}
		else
		{
			TRACE("Start external sorting");
			TRACE("Reading input file into temporary files and sorting them...");
			int numFiles = inFileLength / threadMemLimit + (inFileLength % threadMemLimit == 0 ? 0 : 1);

			SortingThreadInfo* thgroup = new SortingThreadInfo[numFiles];

			for (int block = 0; block < numFiles; ++block)
			{
				string tmpFileName(getTmpFileName());

				int blockSize = 0;
				if (block < numFiles - 1 || inFileLength % threadMemLimit == 0)
					blockSize = threadMemLimit;
				else // the last block
					blockSize = inFileLength % threadMemLimit;

				// Make sure not more than MAX_SORTING_THREADS are running at a time
				// Join the oldest thread before starting a new one
				if (block > MAX_SORTING_THREADS - 1)
					thgroup[block - MAX_SORTING_THREADS].join();

				thgroup[block].alloc(blockSize / sizeof(int));
				thgroup[block].setFileName(tmpFileName);

				vector<int> *v = thgroup[block].getData();
				
				inFile.getFile()->read ((char*)&((*v)[0]), blockSize);

				thgroup[block].start();

				fileQueue.push(new FileAbstraction(tmpFileName.c_str()));
				TRACE(tmpFileName + " is created");
			}

			if (numFiles > MAX_SORTING_THREADS - 1)
				for (int thInd = numFiles - MAX_SORTING_THREADS + 1; thInd < numFiles; thInd++)
					thgroup[thInd].join();

			delete[] thgroup;
			inFile.close();
			TRACE ("Start merging temporary files...");
			
			mergeAllFiles(outFileName, memLimit);
		}
	}
	catch (string msg){
		TRACE("Exception: " + msg);
		return 1;
	}
	TRACE(string("Done. Result written to ") + outFileName);
	return 0;
}

bool checkSorted(const string& outFileName, const string& inFileName = "")
{
	TRACE("\nChecking correctness...");
	InputFile sortedFile(outFileName.c_str());
	try
	{
        size_t sortedFileLength = sortedFile.length();
	
		// If the input file name is provided, also check that lengths of the two files are the same
		if (!inFileName.empty()){
			InputFile inFile(inFileName.c_str());
			if (sortedFileLength != inFile.length())
			{
				inFile.close();
				throw string("Resulting file length is wrong!");
			}
		}
		int firstElem = 0;
		sortedFile.read ((char*)&firstElem, sizeof(int));
		size_t bytesRead = sizeof(int);
		int curElem, prevElem = firstElem;

		while (bytesRead < sortedFileLength)
		{
			sortedFile.read ((char*)&curElem, sizeof(int));
			if (curElem < prevElem)
				throw string("Resulting file is not sorted!");
			prevElem = curElem;
			bytesRead += sizeof(curElem);
		}
	}
	catch (string msg){
		TRACE("Exception: " + msg);
		sortedFile.close();
		return false;
	}
	
	TRACE("Checking OK");
	return true;
}

// Command line parameters: filename and memory_limit
int main(int argc, char* argv[])
{
	try{
		if (argc != 3)
			throw string("Usage: external_sort <filename> <memory_limit_bytes>");
		
		string inFileName (argv[1]);
		string outFileName(inFileName + ".sorted");
		int memLimit = atoi(argv[2]);

		if (memLimit <= 0)
			throw string("Invalid memory limit");

		boost::posix_time::ptime start = boost::posix_time::microsec_clock::local_time();
		
		externalSort(inFileName, outFileName, memLimit);
		
		boost::posix_time::ptime end = boost::posix_time::microsec_clock::local_time();
		boost::posix_time::time_duration td = end - start;
		
		std::cout << "Sorted in " << td.total_milliseconds() / 1000.0 << " seconds" << std::endl;

		// Check if the output file is really sorted
		if (false == checkSorted(outFileName, inFileName))
			throw string("Checking failed!");
	}
	catch (string msg){
		TRACE("Exception: " + msg);
		return 1;
	}

	return 0;
}
