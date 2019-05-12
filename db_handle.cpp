//
// Created by lingo on 19-5-11.
//

#include <iostream>
#include <iomanip>
#include <vector>
#include <fstream>
#include <climits>
#include <unistd.h>
#include "db_handle.h"


Driver::Driver() {
    options_.create_if_missing = true;
    db_ = nullptr;
}

Driver::~Driver() {
    delete db_;
}

void Driver::ParseCommandLine() {

    ShowUsage();

    std::string commandstr; // get a line from console/shell

    std::vector<std::string> paras; // split commandstr and we get parameters

    leveldb::Slice tmp;

    bool ok = true; // no error occurred
    bool quit = false;
    bool opened = false;

    while (true) {
        std::string key;    // parameter key to be passed

        std::string value;  // parameter value to be passed

        std::string paradir; // parameter to be passed to OpenDB

        int execstr = 0; // store which command to execute

        bool parabool = false;  // sync or checksum
        bool invalid = false;   // invalid parameter

        std::string start;  // parameter to be passed to Scan
        std::string limit;  // parameter to be passed to Scan

        std::getline(std::cin, commandstr);
        paras.clear();
        split(commandstr, ' ', paras);  // split and trim string with space char

        for (const auto& str : paras) {
            tmp = str;
            if (tmp.starts_with("opendb")) {
                execstr = 1;
            } else if (tmp.starts_with("closedb")) {
                execstr = 2;
            } else if (tmp.starts_with("deletedb")) {
                execstr = 3;
            } else if (tmp.starts_with("get")) {
                execstr = 4;
            } else if (tmp.starts_with("put")) {
                execstr = 5;
            } else if (tmp.starts_with("update")) {
                execstr = 6;
            } else if (tmp.starts_with("delete")) {
                execstr = 7;
            } else if (tmp.starts_with("scan")) {
                execstr = 8;
            } else if (tmp.starts_with("readseq")) {
                execstr = 9;
            } else if (tmp.starts_with("readreverse")) {
                execstr = 10;
            } else if (tmp.starts_with("quit")) {
                execstr = 11;
            } else if(tmp.starts_with("help")) {
                execstr = 12;
            } else if (tmp.starts_with("load")) {
                execstr = 13;
            } else if (tmp.starts_with("key=")) {
                key = str.substr(4);
            } else if (tmp.starts_with("value=")) {
                value = str.substr(6);
            } else if (tmp.starts_with("sync=")) {
                parabool = str.substr(5) == "true";
            } else if (tmp.starts_with("checksum=")) {
                parabool = str.substr(9) == "true";
            } else if (tmp.starts_with("start=")) {
                start = str.substr(6);
            } else if (tmp.starts_with("limit=")) {
                limit = str.substr(6);
            } else if (tmp.starts_with("dbfilename=")) {
                paradir = str.substr(11);
            } else {
                invalid = true;
                std::cout << "Invalid Parameter" << std::endl;
            }
        }

        // invalid parameters or no execute command
        if (invalid || execstr == 0) continue;
        if (!opened && execstr != 1 && execstr != 11 && execstr != 12) {
            std::cout << "Before any other operation, open DB first" << std::endl;
            continue;
        }

        switch (execstr) {
            case 1:
                if (opened) {
                    std::cout << "Database already opened" << std::endl;
                    break;
                }
                ok = OpenDB(paradir);
                opened = true;
                std::cout << "Database opened in " << directory_ << std::endl;
                break;
            case 2:
                CloseDB();
                opened = false;
                break;
            case 3:
                DeleteDB();
                opened = false;
                break;
            case 4:
                Get(key, parabool);
                break;
            case 5:
                ok = Put(key, value, parabool);
                break;
            case 6:
                ok = Update(key, value, parabool);
                break;
            case 7:
                ok = Delete(key, parabool);
                break;
            case 8:
                ok = Scan(start, limit, parabool);
                break;
            case 9:
                ok = ReadSeq(parabool);
                break;
            case 10:
                ok = ReadReverse(parabool);
                break;
            case 11:
                quit = true;
                break;
            case 12:
                ShowUsage();
                break;
            case 13:
                ok = LoadData();
                break;
        }
        if (!ok || quit) break;
    }
}

bool Driver::OpenDB(const std::string& dir) {
    if (!dir.empty()) {
        directory_ = dir;
    }
    status_ = leveldb::DB::Open(options_, directory_, &db_);
    return CheckStatus();
}

void Driver::CloseDB() {
    delete db_;
    db_ = nullptr;
    std::cout << "Database in " << directory_ << " Closed" << std::endl;
}

void Driver::DeleteDB() {
    CloseDB();
    leveldb::DestroyDB(directory_, options_);
    std::cout << "Database in " << directory_ <<" Deleted" << std::endl;
}

// return true iff found
bool Driver::Get(const std::string& key, bool checksum) {
    assert(db_ != nullptr);
    std::string rep;
    read_options_.verify_checksums = checksum;
    status_ = db_->Get(read_options_, key, &rep);
    return !status_.IsNotFound();
}

// return value in rep
void Driver::Get(const std::string &key, std::string* rep) {
    assert(db_ != nullptr);
    status_ = db_->Get(read_options_, key, rep);
}

bool Driver::Put(const std::string& key, const std::string& value, bool sync) {
    assert(db_ != nullptr);
    write_options_.sync = sync;
    status_ = db_->Put(write_options_, key, value);
    return CheckStatus();
}

// Before update, make sure key exists
bool Driver::Update(const std::string& key, const std::string& value, bool sync) {
    assert(db_ != nullptr);
    std::string rep;
    read_options_.verify_checksums = false;
    status_ = db_->Get(read_options_, key, &rep);
    if (status_.IsNotFound()) {
        std::cout << "Key not found" << std::endl;
        return true;
    } else {
        return Put(key, value, sync);
    }
}

bool Driver::Delete(const std::string& key, bool sync) {
    assert(db_ != nullptr);
    write_options_.sync = sync;
    status_ = db_->Delete(write_options_, key);
    return CheckStatus();
}

bool Driver::Scan(const std::string& start, const std::string& limit, bool checksum) {
    assert(db_ != nullptr);
    ClearKVBuffer();
    read_options_.verify_checksums = checksum;
    leveldb::Iterator* it = db_->NewIterator(read_options_);

    if (!start.empty()) {
        it->Seek(start);
    } else {
        it->SeekToFirst();
    }
    if (!limit.empty()) {
        for (; it->Valid() && it->key().ToString() < limit; it->Next()) {
            keys_.push_back(it->key().ToString());
            values_.push_back(it->value().ToString());
        }
    } else {
        for (; it->Valid(); it->Next()) {
            keys_.push_back(it->key().ToString());
            values_.push_back(it->value().ToString());
        }
    }

    status_ = it->status();
    delete it;
    PrintResults();
    return CheckStatus();
}

bool Driver::ReadSeq(bool checksum) {
    assert(db_ != nullptr);
    ClearKVBuffer();
    read_options_.verify_checksums = checksum;
    leveldb::Iterator* it = db_->NewIterator(read_options_);
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        keys_.push_back(it->key().ToString());
        values_.push_back(it->value().ToString());
    }
    status_ = it->status();
    delete it;
    PrintResults();
    return CheckStatus();
}

bool Driver::ReadReverse(bool checksum) {
    assert(db_ != nullptr);
    ClearKVBuffer();
    read_options_.verify_checksums = checksum;
    leveldb::Iterator* it = db_->NewIterator(read_options_);
    for (it->SeekToLast(); it->Valid(); it->Prev()) {
        keys_.push_back(it->key().ToString());
        values_.push_back(it->value().ToString());
    }
    status_ = it->status();
    delete it;
    PrintResults();
    return CheckStatus();
}

// return false iff error occurred
bool Driver::CheckStatus(bool print) const {
    if (!status_.ok()) {
        std::cout << status_.ToString() << std::endl;
        return false;
    } else {
        if (print) {
            std::cout << "Operation Succeeded" << std::endl;
        }
        return true;
    }
}

void Driver::ClearKVBuffer() {
    keys_.clear();
    values_.clear();
}

void Driver::PrintResults() const {
    for (int i = 0; i <= keys_.size() - 1; i++) {
        std::cout << std::setw(6) << std::left << keys_[i] <<
        std::setw(15) << std::left << values_[i] <<std::endl;
    }
    /*
    std::cout << "K: ";
    for (const auto& key : keys_) {
        std::cout << std::setw(15) << std::left << key;
    }
    std::cout << std::endl;
    std::cout << "V: ";
    for (const auto& value : values_) {
        std::cout << std::setw(15) << std::left << value;
    }
    std::cout << std::endl;
     */
}

void Driver::ShowUsage() const {
    std::cout << "Usage: \n"
                 "1.opendb dbfilename= \n"
                 "2.deletedb \n"
                 "3.closedb \n"
                 "4.get key= checksum= \n"
                 "5.put key= value= sync= \n"
                 "6.update key= value= sync= \n"
                 "7.delete key= sync= \n"
                 "8.scan start= limit= checksum= \n"
                 "9.readseq checksum= \n"
                 "10.readreverse checksum= \n"
                 "11.quit \n"
                 "12.help \n"
                 "13.load \n";
}


static std::string getexepath() {
    char result[PATH_MAX];
    ssize_t  count = readlink("/proc/self/exe", result, PATH_MAX);
    return std::string(result, (count > 0) ? count : 0);
}

bool Driver::LoadData() {
    std::cout << "Working Directory: "<< getexepath() <<std::endl;
    std::ifstream infile("us.txt");
    int id = 1;
    std::string name;
    write_options_.sync = false;
    while (infile >> name) {
        char key[100];
        snprintf(key, sizeof(key), "%04d", id++);
        db_->Put(write_options_, key, name);
        if (!CheckStatus(false)) return false;
    }
    return true;
}
