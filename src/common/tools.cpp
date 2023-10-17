#include <iostream>
#include <fstream>

#include "tools.h"

void AppendToOutputFile(std::string str) {
    if (enable_output_file) {
        std::ofstream outfile;
        outfile.open("output.txt", std::ios::out | std::ios::app);
        outfile << str;
        outfile.close();
    }
}
