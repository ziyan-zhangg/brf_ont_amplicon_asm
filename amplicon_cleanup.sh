#!/bin/bash
# =============================================================================
# amplicon_cleanup.sh v2.0
# Cleans up and reorganises an amplicon run folder on Gadi for client delivery.
#
# Usage: bash amplicon_cleanup.sh [--dry-run] [--keep-raw|--drop-raw]
#                                  <run_folder_name>
#
# Examples:
#   bash amplicon_cleanup.sh amplicon_run_20260508
#   bash amplicon_cleanup.sh --dry-run amplicon_run_20260508
#   bash amplicon_cleanup.sh --drop-raw amplicon_run_20260508
#
# Working directory: /g/data/vz35/amplicon_gadi
#
# Designed for the v1.5 layout produced by amplicon_prep_gadi.py:
#   amplicon_run_<DATE>/
#     <client>/
#       raw/
#         <barcode>/{barcode>.fq.gz, reference/<ref>.fasta}
#       results/
#         <alias>/{alignments, consensus, variants, execution/,
#                  wf-amplicon-report.html, params.json, versions.txt, ...}
#     run_<client>_<barcode>_ref.qsub
#     run_<client>_noref.qsub
#     <client>_sample_sheet_noref.csv
#     run_amplicons.sh
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BASE_DIR="/g/data/vz35/amplicon_gadi"
DRY_RUN=false
# Whether to keep the raw/ folder under each client. Default: keep, since some
# clients want their raw FASTQ back. Override with --drop-raw.
KEEP_RAW=true

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

log_action() {
    if [[ "$DRY_RUN" == true ]]; then
        echo "  [DRY-RUN] $*"
    else
        echo "  $*"
    fi
}

run() {
    if [[ "$DRY_RUN" == true ]]; then
        echo "  [DRY-RUN] would run: $*"
    else
        "$@"
    fi
}

usage() {
    cat <<EOF
Usage: bash amplicon_cleanup.sh [--dry-run] [--keep-raw|--drop-raw] <run_folder_name>

Options:
  --dry-run     Show what would happen without changing anything.
  --keep-raw    Keep <client>/raw/ folders (default).
  --drop-raw    Delete <client>/raw/ folders after cleanup.
                Use only when raw FASTQs are archived elsewhere.

Examples:
  bash amplicon_cleanup.sh amplicon_run_20260508
  bash amplicon_cleanup.sh --dry-run amplicon_run_20260508
  bash amplicon_cleanup.sh --drop-raw amplicon_run_20260508
EOF
}

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
RUN_FOLDER=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --keep-raw)
            KEEP_RAW=true
            shift
            ;;
        --drop-raw)
            KEEP_RAW=false
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        --*)
            echo "ERROR: Unknown option: $1"
            usage
            exit 1
            ;;
        *)
            if [[ -z "$RUN_FOLDER" ]]; then
                RUN_FOLDER="$1"
                shift
            else
                echo "ERROR: Unexpected extra argument: $1"
                usage
                exit 1
            fi
            ;;
    esac
done

if [[ -z "$RUN_FOLDER" ]]; then
    echo "ERROR: <run_folder_name> is required."
    usage
    exit 1
fi

RUN_DIR="${BASE_DIR}/${RUN_FOLDER}"

# Extract the date index from the folder name (e.g. "20260508" from "amplicon_run_20260508")
DATE_INDEX="${RUN_FOLDER##*_}"

if [[ ! -d "$RUN_DIR" ]]; then
    echo "ERROR: Run directory not found: ${RUN_DIR}"
    exit 1
fi

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
echo "============================================================"
echo "  Amplicon cleanup script v2.0"
echo "  Base dir : ${BASE_DIR}"
echo "  Run dir  : ${RUN_DIR}"
echo "  Date idx : ${DATE_INDEX}"
echo "  Keep raw : ${KEEP_RAW}"
if [[ "$DRY_RUN" == true ]]; then
    echo "  Mode     : DRY-RUN — no changes will be made"
else
    echo "  Mode     : LIVE — changes will be applied"
fi
echo "============================================================"
echo ""

# ---------------------------------------------------------------------------
# Step 1 — Remove amplicon_setup scheduler logs from the base directory
# ---------------------------------------------------------------------------
echo "[1/5] Removing amplicon_setup scheduler logs from base directory..."

shopt -s nullglob
setup_logs=("${BASE_DIR}/amplicon_setup.e"* "${BASE_DIR}/amplicon_setup.o"*)
if [[ ${#setup_logs[@]} -gt 0 ]]; then
    for f in "${setup_logs[@]}"; do
        log_action "Removing: $f"
        run rm -f "$f"
    done
else
    echo "  No amplicon_setup.e* / amplicon_setup.o* files found."
fi
shopt -u nullglob

echo ""

# ---------------------------------------------------------------------------
# Step 2 — Remove generated workflow artefacts from inside the run folder
#           - run_*.qsub PBS scripts
#           - run_amplicons.sh top-level launcher
#           - <client>_sample_sheet_noref.csv per-client sheets
#           - ampln_<client>_*.e* / .o* PBS scheduler logs (matches both
#             ampln_<client>_<barcode> ref jobs and ampln_<client>_denovo
#             noref jobs)
# ---------------------------------------------------------------------------
echo "[2/5] Removing PBS scripts, sample sheets, and scheduler logs from ${RUN_DIR}..."

shopt -s nullglob

# run_*.qsub scripts and run_amplicons.sh
run_scripts=("${RUN_DIR}/run_"*)
if [[ ${#run_scripts[@]} -gt 0 ]]; then
    for f in "${run_scripts[@]}"; do
        log_action "Removing: $(basename "$f")"
        run rm -f "$f"
    done
else
    echo "  No run_* scripts found."
fi

# Per-client sample sheets (noref only — they're workflow inputs, not deliverables)
noref_sheets=("${RUN_DIR}"/*_sample_sheet_noref.csv)
if [[ ${#noref_sheets[@]} -gt 0 ]]; then
    for f in "${noref_sheets[@]}"; do
        log_action "Removing: $(basename "$f")"
        run rm -f "$f"
    done
else
    echo "  No *_sample_sheet_noref.csv files found."
fi

# Per-client PBS scheduler logs. Job names from amplicon_prep_gadi.py v1.5:
#   ampln_<client>_<barcode>     (per-barcode ref jobs)
#   ampln_<client>_denovo        (per-client noref job)
# Both match the glob ampln_<client>_*.[eo]* below.
for client_dir in "${RUN_DIR}"/*/; do
    [[ -d "$client_dir" ]] || continue
    client_name="$(basename "$client_dir")"
    # Strip date prefix if folder was already renamed (idempotency)
    if [[ "$client_name" == "${DATE_INDEX}_"* ]]; then
        client_name="${client_name#${DATE_INDEX}_}"
    fi
    job_logs=("${RUN_DIR}/ampln_${client_name}_"*.e* \
              "${RUN_DIR}/ampln_${client_name}_"*.o*)
    if [[ ${#job_logs[@]} -gt 0 ]]; then
        for f in "${job_logs[@]}"; do
            log_action "Removing: $(basename "$f")"
            run rm -f "$f"
        done
    else
        echo "  No ampln_${client_name}_*.e* / .o* files found."
    fi
done

shopt -u nullglob
echo ""

# ---------------------------------------------------------------------------
# Step 3 — Inside each client's results/ folder, clean per-sample noise:
#           - execution/ (per-run Nextflow trace, report.html, timeline.html)
#           - params.json, versions.txt (provenance)
#           - .staging_*/ (orphan dirs from cancelled ref jobs)
#           - .nextflow.log* (if any leaked into results/)
#         Keeps: alignments/, consensus/, variants/,
#                wf-amplicon-report.html, reference_sanitized_seqIDs.fasta(.fai)
# ---------------------------------------------------------------------------
echo "[3/5] Cleaning provenance noise from each client's results/ folder..."

for client_dir in "${RUN_DIR}"/*/; do
    [[ -d "$client_dir" ]] || continue
    client_name="$(basename "$client_dir")"

    # Skip folders already renamed (idempotency)
    if [[ "$client_name" == "${DATE_INDEX}_"* ]]; then
        continue
    fi

    echo "  Client: ${client_name}"

    results_dir="${client_dir}results"
    if [[ ! -d "$results_dir" ]]; then
        echo "    No results/ folder found."
        continue
    fi

    # Warn about any orphan staging dirs (incomplete ref jobs)
    shopt -s nullglob
    orphan_staging=("${results_dir}"/.staging_*)
    shopt -u nullglob
    if [[ ${#orphan_staging[@]} -gt 0 ]]; then
        for d in "${orphan_staging[@]}"; do
            echo "    WARNING: orphan staging dir found: $(basename "$d")"
            log_action "Removing orphan: $(basename "$d")"
            run rm -rf "$d"
        done
    fi

    # Per-alias subdirs: results/<alias>/
    shopt -s nullglob
    alias_dirs=("${results_dir}"/*/)
    shopt -u nullglob

    if [[ ${#alias_dirs[@]} -eq 0 ]]; then
        echo "    No per-sample subfolders found under results/."
        continue
    fi

    for alias_dir in "${alias_dirs[@]}"; do
        [[ -d "$alias_dir" ]] || continue
        alias_name="$(basename "$alias_dir")"
        echo "    Sample: ${alias_name}"

        # Remove execution/ (Nextflow trace + report.html + timeline.html)
        if [[ -d "${alias_dir}execution" ]]; then
            log_action "Removing: ${alias_name}/execution/"
            run rm -rf "${alias_dir}execution"
        fi

        # Remove params.json
        if [[ -f "${alias_dir}params.json" ]]; then
            log_action "Removing: ${alias_name}/params.json"
            run rm -f "${alias_dir}params.json"
        fi

        # Remove versions.txt
        if [[ -f "${alias_dir}versions.txt" ]]; then
            log_action "Removing: ${alias_name}/versions.txt"
            run rm -f "${alias_dir}versions.txt"
        fi

        # Remove any leaked .nextflow.log or .nextflow/ (rare but possible)
        shopt -s nullglob dotglob
        nxf_leftovers=("${alias_dir}".nextflow.log* "${alias_dir}".nextflow)
        shopt -u nullglob dotglob
        for f in "${nxf_leftovers[@]}"; do
            if [[ -e "$f" ]]; then
                log_action "Removing: ${alias_name}/$(basename "$f")"
                run rm -rf "$f"
            fi
        done
    done

    # Some clients also accumulate .nextflow.log / .nextflow/ at the
    # results/ root or the client/ root if Nextflow was launched there.
    # Tidy those too.
    for parent in "$results_dir" "$client_dir"; do
        # Strip trailing slash for cleaner log output
        parent_clean="${parent%/}"
        shopt -s nullglob dotglob
        nxf_top=("$parent_clean"/.nextflow.log* "$parent_clean"/.nextflow)
        shopt -u nullglob dotglob
        for f in "${nxf_top[@]}"; do
            if [[ -e "$f" ]]; then
                log_action "Removing: ${parent_clean#${RUN_DIR}/}/$(basename "$f")"
                run rm -rf "$f"
            fi
        done
    done
done

echo ""

# ---------------------------------------------------------------------------
# Step 4 — Optionally drop <client>/raw/ folders
# ---------------------------------------------------------------------------
echo "[4/5] Handling <client>/raw/ folders..."

if [[ "$KEEP_RAW" == true ]]; then
    echo "  Keeping raw/ folders (--keep-raw, default). Skipping."
else
    for client_dir in "${RUN_DIR}"/*/; do
        [[ -d "$client_dir" ]] || continue
        client_name="$(basename "$client_dir")"

        # Skip already-renamed folders (idempotency)
        if [[ "$client_name" == "${DATE_INDEX}_"* ]]; then
            continue
        fi

        raw_dir="${client_dir}raw"
        if [[ -d "$raw_dir" ]]; then
            log_action "Removing: ${client_name}/raw/"
            run rm -rf "$raw_dir"
        else
            echo "  No raw/ folder for ${client_name}."
        fi
    done
fi

echo ""

# ---------------------------------------------------------------------------
# Step 5 — Rename client folders inside the run directory
#           Pattern: <ClientName> -> <DATE_INDEX>_<ClientName>
# ---------------------------------------------------------------------------
echo "[5/5] Renaming client folders to ${DATE_INDEX}_<ClientName>..."

for client_dir in "${RUN_DIR}"/*/; do
    [[ -d "$client_dir" ]] || continue
    client_name="$(basename "$client_dir")"

    # Skip if already prefixed with the date index
    if [[ "$client_name" == "${DATE_INDEX}_"* ]]; then
        echo "  Already renamed, skipping: ${client_name}"
        continue
    fi

    new_name="${DATE_INDEX}_${client_name}"
    new_path="${RUN_DIR}/${new_name}"

    if [[ -d "$new_path" ]]; then
        echo "  WARNING: Target already exists, skipping: ${new_name}"
        continue
    fi

    log_action "Renaming: ${client_name} -> ${new_name}"
    run mv "${client_dir}" "${new_path}"
done

echo ""
echo "============================================================"
if [[ "$DRY_RUN" == true ]]; then
    echo "  Dry-run complete. No files were modified."
    echo "  Re-run without --dry-run to apply changes."
else
    echo "  Cleanup complete."
fi
echo "============================================================"
