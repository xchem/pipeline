import os
import subprocess

import pandas as pd
import plotly
import plotly.graph_objs as go

from functions import db_functions as dbf
from functions import proasis_api_funcs as paf


def run_edstats(strucid):

    working_directory = os.getcwd()

    print('running edstats for ' + strucid + '...')

    command_string = str('source /dls/science/groups/i04-1/software/pandda-update/ccp4/ccp4-7.0/bin/ccp4.setup-sh')
    process = subprocess.Popen(command_string, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = process.communicate()
    print(out)
    print(err)

    if not os.path.isdir('temp'):
        os.mkdir('temp')
    os.chdir('temp')

    mtz_file = paf.get_struc_mtz(strucid, '.')
    print(mtz_file)
    if mtz_file:
        pdb_file = paf.get_struc_pdb(strucid, str(strucid + '.pdb'))
        print(pdb_file)
        if pdb_file:
            print('writing temporary edstats output...')
            edstats_name = str('edstats_' + str(strucid) + '.out')
            command_string = ('source /dls/science/groups/i04-1/software/pandda-update/ccp4/ccp4-7.0/bin/ccp4.setup-sh;'
                      ' edstats.pl -hklin=' + mtz_file + ' -xyzin=' + pdb_file + ' -out=' +
                              edstats_name + ' > temp.out > /dev/null 2>&1')
            process = subprocess.Popen(command_string, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            out, err = process.communicate()
            print(out)
            print(err)

            # print('reading temporary edstats output...')
            if os.path.isfile(edstats_name):
                with open(edstats_name, 'r') as f:
                    output = f.read().strip().replace('\r\n', '\n').replace('\r', '\n').splitlines()
            else:
                output = None
                raise Exception('No edstats output file found!')

            if output:
                header = output.pop(0).split()
                assert header[:3] == ['RT', 'CI', 'RN'], 'edstats output headers are not as expected! {!s}'.format(output)
                num_fields = len(header)
                header = header[3:]
            else:
                header = []

                # List to be returned
            outputdata = []

            # Process the rest of the data
            if output:
                for line in output:
                    line = line.strip()
                    if not line:
                        continue

                    fields = line.split()
                    if len(fields) != num_fields:
                        raise ValueError(
                            "Error Parsing EDSTATS output: Header & Data rows have different numbers of fields")

                    # Get and process the residue information
                    residue, chain, resnum = fields[:3]
                    try:
                        resnum = int(resnum)
                        inscode = ' '
                    except ValueError:
                        inscode = resnum[-1:]
                        resnum = int(resnum[:-1])

                    # Remove the processed columns
                    fields = fields[3:]

                    # Process the other columns (changing n/a to None and value to int)
                    for i, x in enumerate(fields):
                        if x == 'n/a':
                            fields[i] = None
                        else:
                            try:
                                fields[i] = int(x)
                            except:
                                try:
                                    fields[i] = float(x)
                                except:
                                    fields[i] = x

                    # print residue
                    if 'LIG' in residue:
                        outputdata.append([[residue, chain, resnum], fields])
            # else:
                # raise Exception('No output found!')
        else:
            try:
                os.remove(mtz_file)
                os.remove(str(mtz_file + '.gz'))
            except:
                print('problem removing files')

            raise Exception('No pdb file found for ' + strucid + ' so not running edstats!')
    else:
        try:
            os.remove(mtz_file)
            os.remove(str(mtz_file + '.gz'))
        except:
            print('problem removing files')

        raise Exception('No mtz file found for ' + strucid + ' so not running edstats!')

    try:
        os.remove(pdb_file)
        os.remove(mtz_file)
        os.remove(str(mtz_file + '.gz'))
        os.remove(edstats_name)
    except:
        print('problem removing files')

    os.chdir(working_directory)

    return outputdata, header


def get_project_counts():
    protein_list = []
    conn, c = dbf.connectDB()
    c.execute('select protein from lab')
    rows = c.fetchall()
    for row in rows:
        if not 'null' or 'None' or 'test' in str(row[0]):
            protein_list.append(str(row[0]))
    protein_list = list(set(protein_list))
    counts_dict = {'protein': [], 'mounted': [], 'pandda_hit': [], 'refinement': [], 'comp_chem': [], 'depo': []}
    for protein in protein_list:
        if len(protein) < 1:
            continue
        if 'None' in protein:
            continue
        if 'test' in protein:
            continue
        if 'null' in protein:
            continue
        if 'QC' in protein:
            continue
        counts_dict['protein'].append(protein)
        crystal_list = []
        c.execute('select crystal_name from lab where protein = %s and mounting_result similar to %s', (
            str(protein), '(%Mounted%|%OK%)'))
        rows = c.fetchall()

        for row in rows:
            crystal_list.append(str(row[0]))
        crystal_list = list(set(crystal_list))
        hit = 0

        hits_list = []
        for crystal in crystal_list:
            c.execute('select pandda_hit, crystal_name from dimple where crystal_name = %s', (crystal,))
            rows2 = c.fetchall()
            for row2 in rows2:
                if str(row2[0]) == 'True':
                    hit += 1
                    hits_list.append(str(row2[1]))

        hits_list = list(set(hits_list))

        refinement = []
        comp_chem = []
        depo = []

        for hit_name in hits_list:
            c.execute('select outcome, crystal_name from refinement where outcome similar to %s and crystal_name = %s',
                      ('(%3%|%4%|%5%)', hit_name))
            rows3 = c.fetchall()
            for row3 in rows3:
                if '3' in str(row3[0]):
                    refinement.append(str(row3[1]))
                if '4' in str(row3[0]):
                    comp_chem.append(str(row3[1]))
                if '5' in str(row3[0]):
                    depo.append(str(row3[1]))

        refinement = list(set(refinement))
        counts_dict['refinement'].append(len(refinement))
        comp_chem = list(set(comp_chem))
        counts_dict['comp_chem'].append(len(comp_chem))
        depo = list(set(depo))
        counts_dict['depo'].append(len(depo))
        counts_dict['mounted'].append(len(crystal_list)-hit)
        counts_dict['pandda_hit'].append(hit-len(refinement)-len(comp_chem)-len(depo))

    dataframe = pd.DataFrame.from_dict(counts_dict)
    return dataframe


def project_summary_html(csv_file, html_out):
    table = pd.read_csv(csv_file)

    trace1 = go.Bar(x=table['protein'], y=table['depo'], name='Deposition ready')
    trace2 = go.Bar(x=table['protein'], y=table['comp_chem'], name='Comp. chem. ready')
    trace3 = go.Bar(x=table['protein'], y=table['refinement'], name='In refinement')
    trace4 = go.Bar(x=table['protein'], y=table['pandda_hit'], name='Pandda hit')
    trace5 = go.Bar(x=table['protein'], y=table['mounted'], name='Mounted')

    data = [trace1, trace2, trace3, trace4, trace5]
    layout = go.Layout(barmode='stack', xaxis=dict(tickfont=dict(size=12), showticklabels=True, tickangle=90))

    fig = go.Figure(data=data, layout=layout)
    plotly.offline.plot(fig, filename=html_out, auto_open=False)


def export_ligand_edstats(filename):
    current_path = os.getcwd()
    path = os.path.join(current_path, filename)
    conn, c = dbf.connectDB()
    c.execute("COPY ligand_edstats TO %s DELIMITER ',' CSV HEADER;", (path,))


def draw_violin(dataframe, column):
    title_dict = {'BAa': 'Weighted average Biso',
                  'NPa': 'No of statistically independent grid points covered by atoms',
                  'Ra': 'Real-space R factor (RSR)',
                  'RGa': 'Real-space RG factor (RSRG)',
                  'SRGa': 'Standard uncertainty of RSRG',
                  'CCSa': 'Real-space sample correlation coefficient (RSCC)',
                  'CCPa': 'Real-space "population" correlation coefficient (RSPCC)',
                  'ZCCPa': 'Z-score of real-space correlation coefficient',
                  'ZOa': 'Real-space Zobs score (RSZO)',
                  'ZDa': 'Real-space Zdiff score (RSZD) i.e. max(-RSZD-,RSZD+)',
                  'ZD-a': 'Real-space Zdiff score for negative differences (RSZD-)',
                  'ZD+a': 'Real-space Zdiff score for positive differences (RSZD+)'}

    if str(column) in list(title_dict.keys()):
        title = str(title_dict[column])
    else:
        title = ''

    fig = {
        "data": [{
            "type": 'violin',
            "y": dataframe[str(column)],
            "box": {
                "visible": True
            },
            "line": {
                "color": '#0063cc'
            },
            "meanline": {
                "visible": True
            },
            "fillcolor": '#66b0ff',
            "x0": str(column)
        }],
        "layout": {
            "title": str(title)
        }}

    return fig


def edstats_violin(csv_file, html_root):
    df = pd.read_csv(csv_file)
    for key in list(df.keys()):
        if key in ['index', 'crystal', 'ligand', 'strucid']:
            continue
        fig = draw_violin(df, key)
        out_name = str(key.replace('+', 'plus'))
        out_name = out_name.replace('-', 'minus')
        out_name = out_name + '.html'
        html_out = os.path.join(html_root, out_name)

        plotly.offline.plot(fig, filename=html_out, auto_open=False, validate=False)


