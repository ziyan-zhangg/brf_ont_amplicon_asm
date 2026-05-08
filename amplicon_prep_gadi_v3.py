from argparse import ArgumentParser as AP
from pathlib import Path
import os
from datetime import datetime
from shutil import copy2, rmtree
import gzip

"""
    amplicon_prep_gadi.py v1.5
    This script generates run scripts for assembling amplicon data on Gadi.

    v1.5 changes (per-sample reports, no clobbering):
      - Each per-barcode ref job now writes to its own staging dir
        <client>/results/.staging_<alias>/ to avoid races on the top-level
        report/reference/params files that wf-amplicon writes per
        invocation. After the Nextflow run completes, the script hoists
        the contents up one level to <client>/results/<alias>/ and removes
        the empty staging wrapper. Final result is the clean tree:
            <client>/results/<alias>/{alignments, consensus, variants,
                                      wf-amplicon-report.html, ...}
        with one report per sample, none overwritten.
      - No-ref jobs are unchanged; they run as a single Nextflow invocation
        per client, so there's nothing to race against.

    v1.4 changes (cleaner per-client layout):
      - Raw inputs (FASTQ + reference) now live under <client>/raw/<barcode>/
        instead of <client>/<barcode>/.
      - All wf-amplicon outputs go to <client>/results/. wf-amplicon then
        creates one subdir per sample, named by alias -- so the final
        results tree is flat and human-readable, e.g.
            <client>/results/YC-BcFUL-pro7-7/
            <client>/results/WB-BcFUL-pro7-7/
        No more double-naming (barcode17/YC-BcFUL-pro7-7).
      - Easy to ship a single client by tar'ing <client>/results/.

    v1.3 changes (per-barcode reference handling):
      - wf-amplicon's --reference takes a single FASTA on the CLI, and the
        sample sheet's `ref` column holds *sequence IDs* (not file paths)
        that must already exist inside that FASTA. The previous approach of
        listing per-barcode FASTA paths in a `reference` column did not work.
      - Each barcode that has its own reference FASTA is now run as an
        independent wf-amplicon job (one PBS script per ref-having sample),
        with --reference pointing at that barcode's FASTA and --sample naming
        the alias. No sample sheet is needed for these single-sample runs.
      - All no-ref barcodes for a client are still grouped into one de-novo
        consensus run via a per-client sample sheet, as before.
      - The top-level run_amplicons.sh qsubs every per-barcode-ref script
        plus every per-client no-ref script.
"""


def generate_complete_run_script(top_dir_path, all_script_paths):
    """
    Make a top-level script that launches every PBS script via qsub.
    `all_script_paths` is a flat list of Paths covering both the per-barcode
    ref jobs and the per-client no-ref jobs.
    """
    run_path = Path(top_dir_path) / 'run_amplicons.sh'
    with open(run_path, 'wt') as fout:
        print('#!/bin/bash', file=fout)
        print('', file=fout)
        for sp in all_script_paths:
            print(f'qsub ./{sp.name}', file=fout)
    os.chmod(run_path, 0o755)
    print(f'Generated top-level script {run_path}')


def _write_pbs_header(fout, job_name, email, mem='8GB', ncpus=4, walltime='2:00:00'):
    """Common PBS preamble for all wf-amplicon jobs."""
    print('#!/bin/bash', file=fout)
    print('', file=fout)
    print(f'#PBS -N {job_name}', file=fout)
    print('#PBS -P vz35', file=fout)
    print(f'#PBS -l mem={mem},ncpus={ncpus},walltime={walltime}', file=fout)
    print('#PBS -q biodev', file=fout)
    print('#PBS -l storage=gdata/vz35', file=fout)
    print('#PBS -m abe', file=fout)
    print(f'#PBS -M {email}', file=fout)
    print('#PBS -l wd', file=fout)
    print('', file=fout)


def _write_nxf_env(fout, nxf_base, singularity_cache):
    """Common Nextflow / Singularity environment setup."""
    print('module load java/jdk-17.0.2', file=fout)
    print('module load singularity', file=fout)
    print('', file=fout)
    print(f'export PATH={nxf_base}:$PATH', file=fout)
    print(f'export NXF_HOME={nxf_base}', file=fout)
    print('export NXF_VER=23.10.1', file=fout)
    print(f'export SINGULARITY_CACHEDIR={singularity_cache}', file=fout)
    print('export NXF_SINGULARITY_CACHEDIR=$SINGULARITY_CACHEDIR', file=fout)
    print('export NXF_DISABLE_CHECK_LATEST=true', file=fout)
    print('', file=fout)


def generate_per_barcode_ref_scripts(client_info, client_path, pipeline_path,
        pipeline_version, email, basecaller_cfg=None):
    """
    Generate one PBS script per ref-having barcode. Each script runs
    wf-amplicon in variant calling mode on a single sample with that
    barcode's own reference FASTA.

    Returns a list of script Paths (one per ref-having barcode).
    """
    nxf_base = '/g/data/vz35/amplicon_gadi'
    singularity_cache = f'{nxf_base}/singularity_cache'
    nextflow_path = 'nextflow'
    client_name = client_path.name

    script_paths = []

    for barcode, sample in client_info[client_name].items():
        ref_rel = sample.get('reference')
        if not ref_rel:
            continue  # no-ref samples handled separately

        alias = sample['alias']
        # Each ref job writes to its own staging dir to avoid races on the
        # top-level wf-amplicon-report.html / reference / params files (which
        # the workflow writes per invocation, not per sample). After Nextflow
        # finishes, we hoist the alias subdir's contents up to
        # <client>/results/<alias>/ and remove the staging wrapper.
        # All paths are relative to the top-level amplicon dir (PBS jobs run
        # there because the top-level script is qsub'd from there).
        fastq_in = f'{client_name}/raw/{barcode}'
        staging_dn = f'{client_name}/results/.staging_{alias}'
        final_dn = f'{client_name}/results/{alias}'
        ref_path = str(ref_rel)  # already client/raw/barcode/reference/<file>

        script_path = client_path.parent / f'run_{client_name}_{barcode}_ref.qsub'
        job_name = f'ampln_{client_name}_{barcode}'

        with open(script_path, 'wt') as fout:
            _write_pbs_header(fout, job_name, email)
            _write_nxf_env(fout, nxf_base, singularity_cache)

            print('set -euo pipefail', file=fout)
            print('', file=fout)

            print(f'# wf-amplicon variant calling: {client_name}/{barcode} ({alias})', file=fout)
            print(f'{nextflow_path} run {pipeline_path} -r {pipeline_version} \\', file=fout)
            print(f'  --fastq {fastq_in} \\', file=fout)
            print(f'  --reference {ref_path} \\', file=fout)
            print(f'  --sample {alias} \\', file=fout)
            print(f'  --out_dir {staging_dn} \\', file=fout)
            if basecaller_cfg:
                print(f'  --override_basecaller_cfg {basecaller_cfg} \\', file=fout)
            print(f'  -profile singularity \\', file=fout)
            print(f'  -offline', file=fout)
            print('', file=fout)

            # Hoist staging/<alias>/* and the top-level report/reference/params
            # files up to <client>/results/<alias>/, then drop the staging dir.
            print(f'# Hoist results out of staging into final per-sample dir', file=fout)
            print(f'mkdir -p {final_dn}', file=fout)
            # Move wf-amplicon's per-sample subdir contents (alignments, etc.)
            print(f'if [ -d {staging_dn}/{alias} ]; then', file=fout)
            print(f'  mv {staging_dn}/{alias}/* {final_dn}/', file=fout)
            print(f'  rmdir {staging_dn}/{alias}', file=fout)
            print(f'fi', file=fout)
            # Move top-level files (report, reference, params, versions, execution/)
            print(f'shopt -s dotglob nullglob', file=fout)
            print(f'for f in {staging_dn}/*; do', file=fout)
            print(f'  mv "$f" {final_dn}/', file=fout)
            print(f'done', file=fout)
            print(f'shopt -u dotglob nullglob', file=fout)
            print(f'rmdir {staging_dn}', file=fout)
            print('', file=fout)

        os.chmod(script_path, 0o755)
        script_paths.append(script_path)

    return script_paths


def generate_client_noref_script(client_sample_sheet_noref_path, client_path,
        pipeline_path, pipeline_version, email, basecaller_cfg=None):
    """
    Generate one PBS script per client that runs wf-amplicon in de-novo
    consensus mode on all of that client's no-ref barcodes via a sample
    sheet. Returns the script Path, or None if the client has no no-ref
    samples.
    """
    if client_sample_sheet_noref_path is None:
        return None

    nxf_base = '/g/data/vz35/amplicon_gadi'
    singularity_cache = f'{nxf_base}/singularity_cache'
    nextflow_path = 'nextflow'
    client_name = client_path.name
    fastq_in = f'{client_name}/raw'
    out_dn = f'{client_name}/results'

    script_path = client_path.parent / f'run_{client_name}_noref.qsub'
    job_name = f'ampln_{client_name}_denovo'

    with open(script_path, 'wt') as fout:
        _write_pbs_header(fout, job_name, email, mem='12GB', ncpus=4, walltime='4:00:00')
        _write_nxf_env(fout, nxf_base, singularity_cache)

        print(f'# wf-amplicon de-novo consensus: all no-ref samples for {client_name}', file=fout)
        print(f'{nextflow_path} run {pipeline_path} -r {pipeline_version} \\', file=fout)
        print(f'  --fastq {fastq_in} \\', file=fout)
        print(f'  --out_dir {out_dn} \\', file=fout)
        print(f'  --sample_sheet ./{client_sample_sheet_noref_path.name} \\', file=fout)
        if basecaller_cfg:
            print(f'  --override_basecaller_cfg {basecaller_cfg} \\', file=fout)
        print(f'  -profile singularity \\', file=fout)
        print(f'  -offline', file=fout)
        print('', file=fout)

    os.chmod(script_path, 0o755)
    return script_path


def generate_noref_sample_sheet(client_info: dict, client_path: Path, client_sheet: dict):
    """
    Generate a sample sheet covering only no-ref samples for de-novo consensus
    mode. Uses wf-amplicon's expected column order/names: barcode,alias,type.

    Ref-having samples are NOT in this sheet -- they each get their own
    single-sample wf-amplicon run with --reference + --sample (see
    generate_per_barcode_ref_scripts).

    Returns the sheet Path, or None if the client has no no-ref samples.
    """
    samples_without_references = [
        sn for sn, info in client_info[client_path.name].items()
        if not info.get('reference')
    ]

    if not samples_without_references:
        return None

    sheet_path = client_path.parent / f'{client_path.name}_sample_sheet_noref.csv'
    with open(sheet_path, 'wt') as fout:
        print('barcode,alias,type', file=fout)
        for sample_name in samples_without_references:
            alias = client_sheet[client_path.name][sample_name]['alias']
            barcode = sample_name
            print(f'{barcode},{alias},test_sample', file=fout)

    return sheet_path


def check_fastq_name(fn: str) -> bool:
    """
    Check that the FASTQ file name ends with the expected suffix. Ignore case.
    Returns True if name is good, otherwise False
    """
    suffix = ['.fq', '.fq.gz', '.fastq', '.fastq.gz']
    for s in suffix:
        if str(fn).lower().endswith(s):
            return True
    return False


def check_fasta_name(fn: str) -> bool:
    """
    Check that the FASTA file name ends with the expected suffix. Ignore case
    Returns True if name is good, otherwise False
    """
    suffix = ['.fa', '.fa.gz', '.fasta', '.fasta.gz']
    for s in suffix:
        if str(fn).lower().endswith(s):
            return True
    return False


def rename_fastq_to_bam(fp: str) -> Path | None:
    """
    Replace extension with .bam
    """
    suffix = ['.fq', '.fq.gz', '.fastq', '.fastq.gz']
    for s in suffix:
        if str(fp).lower().endswith(s):
            return Path(str(fp).replace(s, '.bam'))


def parse_samplesheet(samplesheet: str) -> dict:
    """
    Reads a user-provided amplicon sample sheet: e.g.
    client,alias,barcode,ref
    A,amplicon1,barcode21,/path/to/reference.fa
    A,amplicon2,barcode22,ref.fasta
    B,amplicon3,barcode23,
    C,amplicon4,barcode24,

    Returns a dict [client]={barcode:{'alias':'','ref':'','fastqs':[]}}
    """
    if not Path(samplesheet).exists():
        print(f'Samplesheet {samplesheet} does not exist')
        exit(1)
    if not Path(samplesheet).is_file():
        print(f'Samplesheet {samplesheet} is not a file. Did you swap the parameters by mistake?')
        exit(1)

    client_info = {}
    client_barcode_aliases = {}
    with open(samplesheet, 'rt') as f:
        for i, line in enumerate(f):
            cols = line.split(',')
            if i == 0 and cols[0].lower().startswith('client'):
                continue  # header
            if len(cols) < 3:
                continue  # need at least client,alias,barcode
            client = cols[0].strip().replace(' ', '_')
            alias = cols[1].strip().replace(' ', '_')
            barcode = cols[2].strip()
            ref = ''
            if len(cols) > 3:
                ref = cols[3].strip()
            if client not in client_info:
                client_info[client] = {}
                client_barcode_aliases[client] = set()
            if (barcode, alias) not in client_barcode_aliases[client]:
                client_barcode_aliases[client].add((barcode, alias))
            else:
                print(f'barcode {barcode} and alias {alias} are not a unique combination in client {client}')
                exit(1)
            if barcode not in client_info[client]:
                client_info[client][barcode] = {'alias': alias, 'ref': ref, 'fastqs': []}
            else:
                print(f'barcode {barcode} must be unique for client {client}')
                exit(1)
    return client_info


def get_barcode_dirs(p: Path, all_barcodes: set, chosen_dirs: list) -> list:
    """
    Iterate through directories finding all the barcode dirs we want.
    """
    dirs = [x for x in p.iterdir() if x.is_dir()]
    for d in dirs:
        if d.name in ('fastq_pass', 'fastq_fail'):
            bc_dirs = [x for x in d.iterdir() if x.is_dir() if x.name in all_barcodes]
            if bc_dirs:
                chosen_dirs.extend(bc_dirs)
        else:
            get_barcode_dirs(d, all_barcodes, chosen_dirs)
    return chosen_dirs


def parse_input_dirs(prom_dir: str, client_sheet: dict) -> dict:
    """
    Scans a PromethION directory structure:
        Mla7_45_pool/
            -> 20241121_1136_3C_PAW74316_2656d858/
                -> fastq_pass/
                    -> barcode21/ (fastqs)
    Should be able to find everything listed in client_sheet.
    Returns dict source_dirs[client] = {barcode:[list of barcode dirs from fastq_pass and/or fastq_fail]}
    """
    pdp = Path(prom_dir)
    if not pdp.exists():
        print(f"PromethION directory {pdp} does not exist")
        exit(1)
    if not pdp.is_dir():
        print(f"PromethION directory {pdp} is not a directory")
        exit(1)

    source_dirs = {}
    all_barcodes = set()
    for client in client_sheet:
        source_dirs[client] = {}
        for barcode in client_sheet[client]:
            source_dirs[client][barcode] = []
            all_barcodes.add(barcode)

    barcode_dirs = get_barcode_dirs(pdp, all_barcodes, [])
    bcds = {}
    for bcd in barcode_dirs:
        bcds.setdefault(bcd.name, []).append(bcd)
    bcd_names = set(bcds.keys())
    if all_barcodes.difference(bcd_names):
        print(f"Barcodes not found {all_barcodes.difference(bcd_names)}")
        exit(2)
    if bcd_names.difference(all_barcodes):
        print(f"Extra barcodes found {bcd_names.difference(all_barcodes)}")
        exit(2)

    for client in source_dirs:
        for barcode in source_dirs[client]:
            source_dirs[client][barcode] = bcds[barcode]

    return source_dirs


def create_new_structure(plasmid_dir: Path, client_sheet: dict, source_dirs: dict,
                         collapse=True, nodata=False, verbose=False) -> bool:
    """
    Create new amplicon directory tree:

    amplicon_run_20241217/
        -> clientA/
            -> raw/
                -> barcode01/ (fastqs)
                    -> reference/ (fasta)  optional
                -> barcode02/ (fastqs)
            (results/ is created later by wf-amplicon)
        -> clientB/
            -> raw/
                -> barcode03/ (fastqs)

    By default the new barcode directories contain only the collapsed FASTQ file.
    """
    if not plasmid_dir.exists():
        plasmid_dir.mkdir()
    for client in client_sheet:
        p = plasmid_dir / client
        if not p.exists():
            p.mkdir()
        raw_p = p / 'raw'
        if not raw_p.exists():
            raw_p.mkdir()
        for barcode in client_sheet[client]:
            bp = raw_p / barcode
            if not bp.exists():
                bp.mkdir()
            fps = [src_dir / f for src_dir in source_dirs[client][barcode]
                   for f in os.listdir(src_dir) if check_fastq_name(f)]
            if collapse:
                collapse_fp = bp / f'{barcode}.fq.gz'
                if not nodata:
                    with gzip.open(collapse_fp, 'wt') as fout:
                        for fp in fps:
                            if fp.name.lower().endswith('.gz'):
                                if verbose:
                                    print(f'Collapsing {fp} to {collapse_fp}')
                                with gzip.open(fp, 'rt') as f:
                                    for line in f:
                                        if line.strip():
                                            fout.write(line)
                            else:
                                if verbose:
                                    print(f'Collapsing {fp} to {collapse_fp}')
                                with open(fp, 'rt') as f:
                                    for line in f:
                                        if line.strip():
                                            fout.write(line)
            else:
                for fp in fps:
                    if verbose:
                        print(f'Copying {fp} to {bp}')
                    if not nodata:
                        dest_name = f'{fp.parent.parent.name}_{fp.name}'
                        copy2(fp, bp / dest_name)

            ref = client_sheet[client][barcode]['ref']
            if ref:
                ref_dp = bp / 'reference'
                if not ref_dp.exists():
                    ref_dp.mkdir()
                if not nodata:
                    copy2(ref, ref_dp / Path(ref).name)
    return True


def main():
    """
    Generates run scripts for the ONT wf-amplicon pipeline on Gadi.

    User-provided sample sheet has 3 or 4 columns (ref optional):
        client,alias,barcode,ref
        A,amplicon1,barcode21,/path/to/reference.fa
        A,amplicon2,barcode22,ref.fasta
        B,amplicon3,barcode23,
        C,amplicon4,barcode24,

    Scans a PromethION directory structure:
        Mla7_45_pool/
            -> 20241121_1136_3C_PAW74316_2656d858/
                -> fastq_pass/
                    -> barcode21/ (fastqs)

    And creates the required directory structure and scripts:
        amplicon_run_20241217/
            -> clientA/
                -> raw/
                    -> barcode01/ (fastqs)
                        -> reference/ (fasta)  optional
                    -> barcode02/ (fastqs)
                -> results/                       (created by wf-amplicon)
                    -> <alias1>/ ...
                    -> <alias2>/ ...
            -> clientB/
                -> raw/
                    -> barcode03/ (fastqs)
                -> results/
                    -> <alias3>/ ...
            -> run_clientA_barcode01_ref.qsub      (variant calling, 1 sample)
            -> run_clientA_noref.qsub              (de-novo, all no-ref samples)
            -> run_clientB_noref.qsub
            -> clientA_sample_sheet_noref.csv
            -> clientB_sample_sheet_noref.csv
            -> run_amplicons.sh                    (qsubs every job above)
    """
    dt = datetime.today().strftime('%Y%m%d')
    amplicon_dn = f'amplicon_run_{dt}'

    parser = AP()
    parser.add_argument('prom_dir', help='Path to input PromethION sequencing')
    parser.add_argument('-s', '--samplesheet', required=True, help='Path to 3 or 4 column samplesheet to set up experiment')
    parser.add_argument('-p', '--amplicon_dir', default=amplicon_dn, help='Path to output folder containing all client amplicon data')
    parser.add_argument('-v', '--verbose', action='store_true', help='Display more information about the prep process')
    parser.add_argument('-o', '--overwrite', action='store_true', help='Overwrite existing amplicon directory')
    parser.add_argument('--pipeline_path', default='epi2me-labs/wf-amplicon', help='Path to ONT wf-amplicon pipeline')
    parser.add_argument('--pipeline_version', default='v1.2.2', help='wf-amplicon pipeline version')
    parser.add_argument('--basecaller_cfg', default=None, help='Override basecaller config e.g. dna_r10.4.1_e8.2_400bps_sup@v5.0.0')
    parser.add_argument('--no_collapse', action='store_true', help='Disable collapsing FASTQs to a single file for each barcode')
    parser.add_argument('--nodata', action='store_true', help='Run the script without creating any files, for testing purposes')
    parser.add_argument('-e', '--email', required=True, help='Email address for PBS notifications')

    args = parser.parse_args()

    prom_dir = Path(args.prom_dir)
    if not prom_dir.exists():
        print(f'PromethION sequencing directory {prom_dir} does not exist')
        exit(1)

    amplicon_dir = Path(args.amplicon_dir)
    if amplicon_dir.exists() and not args.nodata:
        if not args.overwrite:
            print(f'Amplicon run directory {amplicon_dir} already exists. ' +
                  f'Please delete it, choose to overwrite it, or name a different output directory')
            exit(1)
        else:
            rmtree(amplicon_dir)
    try:
        amplicon_dir.mkdir()
    except FileExistsError:
        if args.nodata:
            pass
        else:
            print(f'Amplicon run directory {amplicon_dir} already exists. Please delete it or name a different output directory')
            exit(1)

    client_sheet = parse_samplesheet(args.samplesheet)
    source_dirs = parse_input_dirs(args.prom_dir, client_sheet)

    collapse_fastqs = not args.no_collapse
    success = create_new_structure(amplicon_dir, client_sheet, source_dirs,
                                   collapse=collapse_fastqs, nodata=args.nodata, verbose=args.verbose)
    if success:
        print(f'Successfully created amplicon directory {amplicon_dir}')

    client_info = {}
    all_script_paths = []  # flat list -- one entry per PBS job

    for client in client_sheet:
        cdir = amplicon_dir / client
        client_info[cdir.name] = {}
        raw_dir = cdir / 'raw'
        sample_dirs = [d for d in raw_dir.glob('*') if d.is_dir() and str(d.name).startswith('barcode')] if raw_dir.exists() else []
        if not sample_dirs:
            print(f'Skipping client {cdir}, no sample directories found')
            continue
        if args.verbose:
            print(f'{cdir} contains samples: {sample_dirs}')

        for sd in sample_dirs:
            barcode = sd.name
            client_info[cdir.name][barcode] = {
                'alias': client_sheet[cdir.name][barcode]['alias'],
            }

            if not args.nodata:
                seq_fns = [fp for fp in sd.glob('*') if fp.is_file() and check_fastq_name(fp.name)]
                if not seq_fns:
                    print(f'No FASTQ (.fq/.fastq/.fq.gz/.fastq.gz files found for client {cdir.name} sample {barcode}')
                    exit(1)
                client_info[cdir.name][barcode]['fastq_files'] = seq_fns
            else:
                client_info[cdir.name][barcode]['fastq_files'] = []

            ref_dir = sd / 'reference'
            if not ref_dir.exists():
                if args.verbose:
                    print(f'Reference directory {ref_dir} not found. Client {cdir.name} sample {barcode}')
                continue
            if not ref_dir.is_dir():
                print(f'Reference path {ref_dir} is not a directory! Client {cdir.name} sample {barcode}')
                exit(1)
            ref_fps = [f for f in ref_dir.glob('*') if f.is_file() and check_fasta_name(f.name)]
            if len(ref_fps) != 1:
                print(f'Expected exactly one reference FASTA in {ref_dir}, found {len(ref_fps)}: {ref_fps}')
                exit(1)
            # Path stored relative to amplicon_dir so PBS scripts (which run in
            # amplicon_dir thanks to `-l wd`) can resolve it directly.
            client_info[cdir.name][barcode]['reference'] = (
                Path(cdir.name) / 'raw' / barcode / 'reference' / ref_fps[0].name
            )

        # Per-barcode ref jobs (one PBS script each)
        ref_script_paths = generate_per_barcode_ref_scripts(
            client_info, cdir, args.pipeline_path, args.pipeline_version,
            args.email, args.basecaller_cfg)
        for sp in ref_script_paths:
            print(f'Created per-barcode ref script {sp.name} for client {cdir.name}')
        all_script_paths.extend(ref_script_paths)

        # Per-client no-ref job (single PBS script with sample sheet)
        noref_sheet_path = generate_noref_sample_sheet(client_info, cdir, client_sheet)
        if noref_sheet_path:
            print(f'Created no-ref sample sheet {noref_sheet_path.name} for client {cdir.name}')
        noref_script_path = generate_client_noref_script(
            noref_sheet_path, cdir, args.pipeline_path, args.pipeline_version,
            args.email, args.basecaller_cfg)
        if noref_script_path:
            print(f'Created no-ref script {noref_script_path.name} for client {cdir.name}')
            all_script_paths.append(noref_script_path)

        if not ref_script_paths and not noref_script_path:
            print(f'WARNING: client {cdir.name} produced no jobs')

    if not all_script_paths:
        print('No jobs were generated. Nothing to launch.')
        exit(1)

    generate_complete_run_script(amplicon_dir, all_script_paths)


if __name__ == '__main__':
    main()
