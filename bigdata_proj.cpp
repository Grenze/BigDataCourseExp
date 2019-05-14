
#include <fstream>
#include <iostream>
#include <vector>
#include <map>
#include <algorithm>
#include <cstdlib>
#include <ctime>
#include "proj_service.h"


int main() {
	Service s_;
	s_.ParseCommandLine();
	/*
	s_.LoadAndInit("../dataset/transaction.txt");
	s_.PrintTopKGoodsWithin("0000001", "0011000", 10);
	std::cout << std::endl;
	s_.PrintTopKGoodsBoughtBy("Abel", 10);
	std::cout << std::endl;
	s_.PrintTopKUsrWhoBought("Pen", 10);*/

	return 0;
}
