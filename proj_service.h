
#include <map>
#include <string>
#include <vector>
#include <fstream>
#include "db_handle.h"

// The key-value 's form
// TimeStamp  :    "!TimeStamp"    "UsrName,GoodsName"
// UsrName    :    "#UsrName"      "GoodsName1,GoodsName2,..."
// GoodsName  :    "$GoodsName"    "UsrName1,UsrName2,..."

class Service {
public:

    Service();

    ~Service();

	// Parse user's command and execute it
	void ParseCommandLine();

	void PrintUsage();

	// Load the transaction data and write them into the LevelDB
	void LoadAndInit(const char* filepath);  
	// Giving the TimeInterval [ ts_ , te_ ) 
	// Print the Top K Goods with the highest sales volume during [ ts_ , te_ ) 
	void PrintTopKGoodsWithin(const std::string& ts_, const std::string& te_, const int& K);  
	// Giving the UserName
	// Print all the Top K goods that "UserName" has bought with the most times
	void PrintTopKGoodsBoughtBy(const std::string& usrname, const int& K);
	// Giving the GoodsName
	// Print the Top K uers who had bought "GoodsName" with the most times
	void PrintTopKUsrWhoBought(const std::string& goodsname, const int& K);
private:
    Driver* driver_;
	//std::map<std::string, std::string> virtualdb;    // Virtual LevelDB , need to be replaced
};
