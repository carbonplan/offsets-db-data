import argparse
import json

import pandas as pd


def main():
    parser = argparse.ArgumentParser(
        description='Extract project types from latest version of berkeley carbon project data',
    )
    parser.add_argument('filename', help='Input filename to process')

    args = parser.parse_args()

    # this is surprisingly slow? openpyxl is doing some _work_
    project_data = pd.read_excel(
        args.filename, sheet_name='PROJECTS', skiprows=3, usecols=['Project ID', ' Type']
    )

    def _fix_gld_ids(s: str) -> str:
        if s.startswith('GS'):
            return f'GLD{s[2:]}'
        else:
            return s

    out_d = project_data.dropna().set_index('Project ID')[' Type'].to_dict()
    out_d = {_fix_gld_ids(k): v.lower() for k, v in out_d.items()}
    out_f = '/tmp/berkeley-project-types.json'
    with open(out_f, 'w') as f:
        print(f'Writing project types to {out_f}')
        json.dump(out_d, f, indent=1)


if __name__ == '__main__':
    main()
