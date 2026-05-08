# amplicon_prep_gadi

A Python script that takes a PromethION sequencing run and a sample sheet, then generates everything needed to run [`epi2me-labs/wf-amplicon`](https://github.com/epi2me-labs/wf-amplicon) on Gadi (NCI's HPC). Designed for a sequencing-facility workflow where one run contains samples from multiple clients, with a mix of variant-calling (reference provided) and de-novo (no reference) samples.

## What it does

Given a PromethION output directory and a CSV sample sheet, the script:

1. Builds a clean per-client directory tree on Gadi.
2. Collapses each barcode's `fastq_pass` (and `fastq_fail`) FASTQ files into a single `<barcode>.fq.gz`. This is because sequences generated from Rapid Prep is usually shorter.
3. Copies the reference FASTA into each ref-having barcode's folder.
4. Generates one PBS job script per ref sample (variant calling mode).
5. Generates one PBS job script per client for all that client's no-ref samples (de-novo mode).
6. Generates a top-level `run_amplicons.sh` that `qsub`s all jobs at once.

## Two assembly modes

`wf-amplicon` runs in one of two modes, decided per sample by whether a reference was provided in the sample sheet.

### Variant calling mode (reference provided)

Used when the client supplies a reference FASTA for a barcode. The workflow aligns reads against that reference using minimap2, then calls variants with Medaka and produces a consensus.

Each ref sample is run as its **own independent Nextflow invocation** — one PBS job per barcode. This is necessary because `wf-amplicon` takes a single `--reference` FASTA on the command line, and we want each barcode aligned only against its own reference, not a pooled FASTA shared across clients.

Outputs per ref sample include:
- `alignments/` — sorted BAM + index against the (sanitized) reference
- `variants/` — Medaka-annotated VCF
- `consensus/` — Medaka consensus FASTA
- `reference_sanitized_seqIDs.fasta` — the reference with whitespace-safe IDs
- `wf-amplicon-report.html` — interactive QC + variant report for this sample
- `params.json`, `versions.txt`, `execution/` — provenance

### De-novo assembly mode (no reference)

Used when the sample sheet leaves the reference column blank. The workflow filters reads by length/quality, then runs miniasm to produce a draft assembly, polishes with Medaka, and reports back QC.

All of one client's no-ref samples are run together in a **single Nextflow invocation** driven by a per-client sample sheet. This is fine because no-ref jobs don't compete for any per-invocation output files — they coexist cleanly under one `--out_dir`.

Outputs per no-ref sample include:
- `consensus/` — polished de-novo assembly per sample
- A single client-level `wf-amplicon-report.html` covering all that client's de-novo samples

### Index
**Variant Calling Options**: min_coverage = 20 (Only variants covered by more than this number of reads are reported in the resulting VCF file.)

**De-novo Consensus Options**: average coverage > 150X. Usually 1500 reads per amplicon should thus be enough in the vast majority of cases.



## Output directory layout

For a run containing a client called `Client`:

```
amplicon_run_YYYYMMDD/
├── Client/
│   ├── raw/
│   │   ├── barcode17/
│   │   │   ├── barcode17.fq.gz
│   │   │   └── reference/
│   │   │       └── BcFUL_YC_pro77.fasta
│   │   ├── barcode18/
│   │   │   └── ...
│   │   └── barcode29/
│   └── results/
│       ├── YC-BcFUL-pro7-7/        ← named by alias, not barcode
│       │   ├── alignments/
│       │   ├── consensus/
│       │   ├── variants/
│       │   ├── execution/
│       │   ├── reference_sanitized_seqIDs.fasta
│       │   ├── params.json
│       │   ├── versions.txt
│       │   └── wf-amplicon-report.html
│       ├── WB-BcFUL-pro7-7/
│       │   └── ...
│       └── ...
├── Client_sample_sheet_noref.csv     (only if client has no-ref samples)
├── run_Client_barcode17_ref.qsub     (one per ref barcode)
├── run_Client_barcode18_ref.qsub
├── ...
├── run_Client_noref.qsub             (one per client, if applicable)
└── run_amplicons.sh                        (qsubs every job above)
```

Inputs live under `<client>/raw/`, results under `<client>/results/`. To deliver a client their data, `tar czf <client>_results.tar.gz <client>/results` is enough — one report per sample, no clobbering, no internal barcode IDs to confuse them.

## Usage

```bash
python3 amplicon_prep_gadi.py <prom_dir> -s <samplesheet.csv> -e <email> [options]
```

### Required arguments

- `prom_dir` — path to the PromethION run directory (the script walks it looking for `fastq_pass/` and `fastq_fail/` folders containing barcoded reads)
- `-s, --samplesheet` — path to the CSV sample sheet (see format below)
- `-e, --email` — email address for PBS notifications

### Common options

- `-p, --amplicon_dir` — name of the output directory (defaults to `amplicon_run_<YYYYMMDD>`)
- `-o, --overwrite` — wipe the output directory if it already exists
- `-v, --verbose` — print details of FASTQ collapsing and reference resolution
- `--pipeline_version` — `wf-amplicon` version (default `v1.2.2`)
- `--basecaller_cfg` — override basecaller config string, e.g. `dna_r10.4.1_e8.2_400bps_sup@v5.0.0`
- `--no_collapse` — keep individual FASTQ files instead of merging into one per barcode
- `--nodata` — generate the directory structure and PBS scripts without copying any data (useful for dry runs)

## Sample sheet format

A CSV with 3 or 4 columns, header optional:

```csv
client,alias,barcode,ref
Client,YC-BcFUL-pro7-7,barcode17,/path/to/BcFUL_YC_pro77.fasta
Client,WB-BcFUL-pro7-7,barcode18,/path/to/BcFUL_WB_pro77.fasta
Client,unknown_amplicon_3,barcode23,
Other_Client,plasmid_X,barcode01,
```

Column meanings:

- **client** — directory name on disk for this client's data. Spaces become underscores.
- **alias** — human-readable sample name. Used in reports, output paths, and BAM headers. Must be unique per client.
- **barcode** — ONT barcode folder name (`barcode01`, `barcode02`, ... `barcode96`).
- **ref** — path to a reference FASTA. Leave blank for de-novo mode. Path can be absolute or relative to where you run the script.

The same client can have a mix of ref and no-ref rows; each is routed to the appropriate mode.

## Reference file
Reference file need to be a fasta file. If more than one reference need to be assigned to one barcoded sample. Concatenate them as wf-amplicon natively reads them all.

```bash
awk 'FNR==1 && NR!=1 {print ""} {print}' ref1.fasta ref2.fasta ref3.fasta > concatenated_refs.fasta
```


## Running the jobs

After the script finishes:

```bash
cd amplicon_run_<YYYYMMDD>
./run_amplicons.sh
```

This `qsub`s every PBS script. They run independently on the `biodev` queue:

- Each ref job: `mem=8GB, ncpus=4, walltime=2:00:00` (single sample)
- Each client's no-ref job: `mem=12GB, ncpus=4, walltime=4:00:00` (covers all that client's de-novo samples)

Failures are isolated per job, so you can re-`qsub` an individual script to retry a single sample without re-running everything.


## Known limitations

- `--nodata` mode skips copying reference FASTAs, so the post-build scan for references will fail if any sample sheet rows have a non-blank `ref`. Use `--nodata` for no-ref-only test runs, or expect the failure to come from `Expected exactly one reference FASTA in ...`.
- Each ref-having barcode must have **exactly one** FASTA file under its `reference/` directory. The script enforces this and exits if it finds zero or more than one.
- If you `qstat`-cancel a ref job mid-hoist (a sub-second window), a leftover `.staging_<alias>/` may remain. Just `rm -rf` it and re-`qsub` the script — the script will recreate everything.
