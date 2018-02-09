import proasis_api_funcs as paf
import os

def run_edstats(strucid):

    working_directory = os.getcwd()

    print('running edstats for ' + strucid + '...')

    os.system('source /dls/science/groups/i04-1/software/pandda-update/ccp4/ccp4-7.0/bin/ccp4.setup-sh')
    if not os.path.isdir('temp'):
        os.mkdir('temp')
    os.chdir('temp')

    mtz_file = paf.get_struc_mtz(strucid, '.')
    if mtz_file:
        pdb_file = paf.get_struc_pdb(strucid, str(strucid + '.pdb'))
        print pdb_file
        if pdb_file:
            print('writing temporary edstats output...')
            edstats_name = str('edstats_' + str(strucid) + '.out')
            os.system('source /dls/science/groups/i04-1/software/pandda-update/ccp4/ccp4-7.0/bin/ccp4.setup-sh; '
                      'edstats.pl -hklin=' + mtz_file + ' -xyzin=' + pdb_file + ' -out=' + edstats_name + ' > temp.out'
                                                                                                          ' > /dev/null 2>&1')
            # print('reading temporary edstats output...')
            if os.path.isfile(edstats_name):
                with open(edstats_name, 'r') as f:
                    output = f.read().strip().replace('\r\n', '\n').replace('\r', '\n').splitlines()
            else:
                output=None
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
                        raise ValueError("Error Parsing EDSTATS output: Header & Data rows have different numbers of fields")

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
            #else:
                #raise Exception('No output found!')
        else:
            try:
                os.system('rm ' + mtz_file)
                os.system('rm ' + mtz_file + '.gz')
            except:
                print('problem removing files')

            raise Exception('No pdb file found for ' + strucid + ' so not running edstats!')
    else:
        try:
            os.system('rm ' + mtz_file)
            os.system('rm ' + mtz_file + '.gz')
        except:
            print('problem removing files')

        raise Exception('No mtz file found for ' + strucid + ' so not running edstats!')

    try:
        os.system('rm ' + pdb_file)
        os.system('rm ' + mtz_file)
        os.system('rm ' + mtz_file + '.gz')
        os.system('rm ' + edstats_name)
    except:
        print('problem removing files')

    os.chdir(working_directory)

    return outputdata, header