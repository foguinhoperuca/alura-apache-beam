.PHONY: run

clean:
	@rm -f data/final_analyze-00000-of-00001.csv

run-full: clean
	@clear
	@date
	@time python3 main.py --dataset full
	@echo ""
	@echo "--- run finished"
	@echo ""

run-sample: clean
	@clear
	@date
	@python3 main.py
	@echo ""
	@echo "--- run finished"
	@echo ""

sample: run-sample
	@cat data/final_analyze-00000-of-00001.csv

full: run-full
	@cat data/final_analyze-00000-of-00001.csv
	@date
