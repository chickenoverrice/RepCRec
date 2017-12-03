# RepCRec is the final project for the course CSCI-GA.2434-001 Advanced Database Systems in Fall 2017 at New York University.
# RepCRec was written in Python3 by Yuzheng Wu and Zhe Xu.

====How to run the program====
To run the program, use the following command:

python3 </path/to/the/script>main.py <--verbose> <--f /path/to/your/test/file>

Option:
	1. --verbose: If chosen, the program prints out detailed information when executing each instruction.
	2. --f /path/to/your/test/file: If chosen and a valid file path is provided, the program executes instructions
		from an input file. If not chosen, the program reads instructions from stdin.
		
For example, if you are currently in the src folder and you want to run a test file in tests fold, the run:
python3 main.py --f ../tests/test0.txt

====Test cases====
Test cases 1-16 and 29-31 are from the course website.
Test cases 0, 17-28 are customized cases.