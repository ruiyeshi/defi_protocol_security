.PHONY: all contracts slither mythril incidents labels severity dynamic clean

DATE := $(shell date +"%Y%m%d_%H%M")
OUTDIR := outputs/$(DATE)
LOGDIR := logs/$(DATE)

all: setup contracts slither mythril incidents labels severity dynamic

setup:
	@mkdir -p $(OUTDIR) $(LOGDIR) data_raw/scans_slither data_raw/scans_mythril data_raw/scans_echidna
	@echo "🗂️ Output directory: $(OUTDIR)"
	@echo "🗒️ Logs directory: $(LOGDIR)"

contracts:
	@echo "🚀 [1/7] Fetching verified contracts..."
	@python fetch_contracts_expanded.py 2>&1 | tee $(LOGDIR)/contracts.log

slither:
	@echo "🧠 [2/7] Running Slither..."
	@python run_slither_batch.py 2>&1 | tee $(LOGDIR)/slither.log

mythril:
	@echo "🔮 [3/7] Running Mythril..."
	@python run_mythril_batch.py 2>&1 | tee $(LOGDIR)/mythril.log || true

incidents:
	@echo "🧾 [4/7] Fetching exploit datasets..."
	@python fetch_exploit_datasets.py 2>&1 | tee $(LOGDIR)/incidents.log

labels:
	@echo "🧩 [5/7] Merging exploit ground truth..."
	@python merge_exploit_groundtruth.py 2>&1 | tee $(LOGDIR)/labels.log

severity:
	@echo "📊 [6/7] Computing severity scores..."
	@python compute_severity_scores.py 2>&1 | tee $(LOGDIR)/severity.log

dynamic:
	@echo "⚙️ [7/7] Running Echidna dynamic tests..."
	@python run_dynamic_batch.py 2>&1 | tee $(LOGDIR)/echidna.log

clean:
	@echo "🧹 Cleaning intermediate files..."
	rm -rf outputs/* logs/* data_raw/scans_slither/*.json data_raw/scans_mythril/*.json data_raw/scans_echidna/*.json