
#include <map>
#include <string>
#include <iostream>
#include <fstream>
#include <algorithm>
#include "proj_service.h"

// The key-value 's form
// TimeStamp  :    "!TimeStamp"    "UsrName,GoodsName"
// UsrName    :    "#UsrName"      "GoodsName1,GoodsName2,..."
// GoodsName  :    "$GoodsName"    "UsrName1,UsrName2,..."

Service::Service() :driver_(new Driver()) {
    driver_->OpenDB("/dev/shm");   // tempfs will be faster
}

Service::~Service() {
    driver_->CloseDB();
    delete driver_;
}

void Service::ParseCommandLine() {

    std::string commandstr; // get a line from console/shell

    std::vector<std::string> paras; // split commandstr and we get parameters

    leveldb::Slice tmp;

    bool ok = true; // no error occurred
    bool quit = false;

    while (true) {

        std::string ts_;
        std::string te_;
        int k = 1;
        std::string usrname;
        std::string goodsname;
        int exec_code = 0;

        std::getline(std::cin, commandstr);
        paras.clear();
        split(commandstr, ' ', paras);  // split and trim string with space char

        bool invalid = false;   // invalid parameter

        for (const auto& str : paras) {
            tmp = str;
            if (tmp.starts_with("PrintTopKGoodsWithin")) {
                exec_code = 1;
            } else if (tmp.starts_with("PrintTopKGoodsBoughtBy")) {
                exec_code = 2;
            } else if (tmp.starts_with("PrintTopKUsrWhoBought")) {
                exec_code = 3;
            } else if (tmp.starts_with("start_time=")) {
                ts_ = str.substr(11);
            } else if (tmp.starts_with("end_time=")) {
                te_ = str.substr(9);
            } else if (tmp.starts_with("username=")) {
                usrname = str.substr(9);
            } else if (tmp.starts_with("goodsname=")) {
                goodsname = str.substr(10);
            } else if (tmp.starts_with("K=")) {
                k = std::stoi(str.substr(2));
            } else if (tmp.starts_with("Quit")) {
                exec_code = 4;
            } else if (tmp.starts_with("Help")) {
                exec_code = 5;
            } else if (tmp.starts_with("LoadData")) {
                exec_code = 6;
            }
            else {
                invalid = true;
                std::cout << "Invalid Parameter" << std::endl;
            }
        }

        // invalid parameters or no execute command
        if (invalid || exec_code == 0) continue;

        switch (exec_code) {
            case 1:
                PrintTopKGoodsWithin(ts_, te_, k);
                break;
            case 2:
                PrintTopKGoodsBoughtBy(usrname, k);
                break;
            case 3:
                PrintTopKUsrWhoBought(goodsname ,k);
                break;
            case 4:
                quit = true;
                break;
            case 5:
                PrintUsage();
                break;
            case 6:
                LoadAndInit("dataset/transaction.txt");
                break;
        }
        if (quit) break;
    }

}

void Service::PrintUsage() {
    std::cout << "Usage: \n"
                 "1.PrintTopKGoodsWithin start_time= end_time= \n"
                 "2.PrintTopKGoodsBoughtBy username= K= \n"
                 "3.PrintTopKUsrWhoBought  goodsname= K= \n"
                 "4.Quit \n"
                 "5.Help \n"
                 "6.LoadData \n";
}

void Service::LoadAndInit(const char* filepath) {     // Load the transaction data and write them into the LevelDB
	std::ifstream f_trans_(filepath);
	std::string timestamp, username, goodsname;
	std::string key, value;
	while (!f_trans_.eof()) {
		f_trans_ >> timestamp >> username >> goodsname;
		// key: TimeStamp
		key = "!" + timestamp;
		value = username + "," + goodsname;
		//virtualdb[key] = value;     // need to be replaced by Put(key, value) in LevelDB
		driver_->Put(key, value);
		// key: UsrName
		key = "#" + username;
		//if (virtualdb.find(key) != virtualdb.end()) {    // to know whether the key has been inserted to the db, need to be replaced by other operations in leveldb
		if (driver_->Get(key)) {
			//value = virtualdb[key] + "," + goodsname;
			std::string rep;
			driver_->Get(key, &rep);
			value = rep + "," + goodsname;
		}else {
			value = goodsname;
		}
		//virtualdb[key] = value;     // need to be replaced by Put(key, value) in LevelDB
        driver_->Put(key, value);
		// key: GoodsName
		key = "$" + goodsname;
		//if (virtualdb.find(key) != virtualdb.end()) {    // to know whether the key has been inserted to the db, need to be replaced by other operations in leveldb
		if (driver_->Get(key)) {
			//value = virtualdb[key] + "," + username;
            std::string rep;
            driver_->Get(key, &rep);
            value = rep + "," + username;
		}
		else {
			value = username;
		}
		//virtualdb[key] = value;     // need to be replaced by Put(key, value) in LevelDB
        driver_->Put(key, value);
    }
	f_trans_.close();
}

std::vector<std::string> split(const std::string& s, const std::string& sep)
{
	std::vector<std::string> v;
	std::string::size_type pos1, pos2;
	pos2 = s.find(sep);
	pos1 = 0;
	while (std::string::npos != pos2)
	{
		v.push_back(s.substr(pos1, pos2 - pos1));

		pos1 = pos2 + sep.size();
		pos2 = s.find(sep, pos1);
	}
	if (pos1 != s.length())
		v.push_back(s.substr(pos1));
	return v;
}

bool Cmp(const std::pair<std::string, int>& a, const std::pair<std::string, int>& b) {
	return a.second > b.second;
}

void Service::PrintTopKGoodsWithin(const std::string& ts_, const std::string& te_, const int& K) {
	std::vector<std::string> valuelist;                                                    //-----------------------------------------------------
	//std::map<std::string, std::string>::iterator it_ = virtualdb.lower_bound("!" + ts_);   // |                                                                              // |

	//while (it_ != virtualdb.end() && it_->first < "!" + te_) {                             // |Get all values in TimeInterval [ ts_ , te_ )
	//	valuelist.push_back(it_->second);                                                  // |Need to be replaced by Scan(ts_ , te_) in LevelDB
	//	it_++;                                                                             // |The result is a vector which only stores the values
	//}                                                                                      //-----------------------------------------------------

    leveldb::Iterator* it_ = driver_->db_->NewIterator(leveldb::ReadOptions());
    for (it_->Seek("!" + ts_); it_->Valid() && it_->key().ToString() < "!" + te_; it_->Next()) {
        valuelist.push_back(it_->value().ToString());
    }

	// Compute the the Top K Goods with the highest sales volume during [ ts_ , te_ ) 
	if (valuelist.size() == 0) {
		std::cout << "No Goods in the TimeInterval!" << std::endl;
		return;
	}
	std::map<std::string, int> count;
	std::string goodsname;
	for (int i = 0; i < valuelist.size(); i++) {
		goodsname = split(valuelist[i], ",")[1];
		if (count.find(goodsname) != count.end()) {
			count[goodsname] += 1;
		}
		else {
			count[goodsname] = 1;
		}
	}
	std::vector<std::pair<std::string, int>> rst;
	for (std::map<std::string, int>::iterator count_iter = count.begin(); count_iter != count.end(); count_iter++) {
		rst.push_back(std::make_pair(count_iter->first, count_iter->second));
	}
	sort(rst.begin(), rst.end(), Cmp);
	// Print
	for (int i = 0; i < std::min(K, (int)rst.size()); i++) {
		std::cout << "  | Top " << i + 1 << "\tSales volume:" << rst[i].second << "\tGoodsName: " << rst[i].first << std::endl;
	}
}

void Service::PrintTopKGoodsBoughtBy(const std::string& usrname, const int& K) {
	std::string value_;                                        //---------------------------------------------------                                                                             // |
	//if (virtualdb.find("#" + usrname)!= virtualdb.end()) {     // |
	//	value_ = virtualdb["#" + usrname];                     // |
	//}
	if (driver_->Get("#" + usrname)) {
        std::string rep;
        driver_->Get("#" + usrname, &rep);
        value_ = rep;
    }
	else {                                                    // |
		std::cout << usrname << " not exists!" << std::endl;   // |Get the value with key: #usrname
		return;                                                // |Need to be replaced by Get(#usrname) in LevelDB
	}                                                          //---------------------------------------------------

	// Print all the goods that "usrname" has bought with the times
	std::vector<std::string> goods = split(value_, ",");
	std::map<std::string, int> count;
	for (int i = 0; i < goods.size(); i++) {
		if (count.find(goods[i]) != count.end()) {
			count[goods[i]] += 1;
		}
		else {
			count[goods[i]] = 1;
		}
	}
	std::vector<std::pair<std::string, int>> rst;
	for (std::map<std::string, int>::iterator count_iter = count.begin(); count_iter != count.end(); count_iter++) {
		rst.push_back(std::make_pair(count_iter->first, count_iter->second));
	}
	sort(rst.begin(), rst.end(), Cmp);
	// Print
	for (int i = 0; i < std::min(K, (int)rst.size()); i++) {
		std::cout << "  | Top " << i + 1 << "\tBought Times:" << rst[i].second << "  \tGoodsName: " << rst[i].first << std::endl;
	}
}

void Service::PrintTopKUsrWhoBought(const std::string& goodsname, const int& K) {
	std::string value_;                                           //---------------------------------------------------                                                                             // |
	//if (virtualdb.find("$" + goodsname) != virtualdb.end()) {     // |
	//	value_ = virtualdb["$" + goodsname];                      // |
	//}
    if (driver_->Get("$" + goodsname)) {
        std::string rep;
        driver_->Get("$" + goodsname, &rep);
        value_ = rep;
    }
	else {                                                       // |
		std::cout << goodsname << " not exists!" << std::endl;    // |Get the value with key: $goodsname
		return;                                                   // |Need to be replaced by Get($goodsname) in LevelDB
	}                                                             //---------------------------------------------------

	// Print the Top K uers who had bought "GoodsName" with the most times
	std::vector<std::string> usrs = split(value_, ",");
	std::map<std::string, int> count;
	for (int i = 0; i < usrs.size(); i++) {
		if (count.find(usrs[i]) != count.end()) {
			count[usrs[i]] += 1;
		}
		else {
			count[usrs[i]] = 1;
		}
	}
	std::vector<std::pair<std::string, int>> rst;
	for (std::map<std::string, int>::iterator count_iter = count.begin(); count_iter != count.end(); count_iter++) {
		rst.push_back(std::make_pair(count_iter->first, count_iter->second));
	}
	sort(rst.begin(), rst.end(), Cmp);
	// Print
	for (int i = 0; i < std::min(K, (int)rst.size()); i++) {
		std::cout << "  | Top " << i + 1 << "\tBought Times:" << rst[i].second << "  \tUsrName: " << rst[i].first << std::endl;
	}
}