import pandas as pd
import numpy as np
import datetime
import os
import subprocess

history = pd.read_csv('elf_history.txt')
def ego_str_date(s):
    v = s.split(' ')[0]
    return datetime.datetime.strptime(v, "%m/%d/%Y").date()
def out_str_date(s):
    return datetime.datetime.strptime(s, "%Y-%m-%d").date()
history['datel'] = history['date'].apply(ego_str_date)


def mcalculate_error(filename, field):
    '''
    calculate average absolute error for a given forecast.
    we assume that in history data we have the same dates with real forecast.
    '''
    
    frame = pd.read_csv(filename)
    frame['datel'] = frame['date'].apply(out_str_date)
    # frame.set_index(['Orgcode','date', 'hour'])
    joined = pd.merge(history, frame, left_on = ['Orgcode','datel', 'hour'], right_on  = ['Orgcode','datel', 'hour'])    
    joined['error'] = (joined[field] - joined['Load'])/joined['Load']
    joined['error_abs'] = np.abs(joined['error'])
    
    final_error = joined['error'].mean()
    v_abs = np.abs(joined['error'])
    final_error_abs = v_abs.mean()
    final_error_abs_max = v_abs.max()
    return (final_error_abs, final_error_abs_max, final_error, joined)

def param_grid_size(paramgrid):
    total_permutations = 1
    for param, values in paramgrid.items():
        l = len(values)
        if (l==0):
            l = 2                
        total_permutations = total_permutations * l
    return total_permutations
    

varying_params_year = {
'params': [
    'elf_params__3_28_1__20162017_2018.txt',
    'elf_params__5_28_1__20162017_2018.txt',
],
'SEA': ['0', '0.5', '1.0'],
'backcast_dates': ['365'],
'backcast_agg_rule': ['hour,bd,month', 'hour,bd,season', 'hour,bd,dow,month', 'hour,bd,dow,season'],
'bestmatch_first_guess': ['temp', 'dd'],
'backcast_error_application': ['relative', 'absolute'],
'backcast_error_source':  ['before_smooth', 'smoothed'],
'match_rule': ['DowBD', 'BD'],    
'exclude_today_from_match': [],
'round_temp_for_miss': []    
}

varying_params_2w = {
'params': [
#    'elf_params__3_21_0__20162018m3_2018april2wl.txt',
#    'elf_params__3_21_1__20162018m3_2018april2wl.txt',
#    'elf_params__3_28_0__20162018m3_2018april2wl.txt',
#    'elf_params__3_28_1__20162018m3_2018april2wl.txt',
#    'elf_params__3_35_0__20162018m3_2018april2wl.txt',
#    'elf_params__3_35_1__20162018m3_2018april2wl.txt',
#    'elf_params__3_60_0__20162018m3_2018april2wl.txt',
#    'elf_params__3_60_1__20162018m3_2018april2wl.txt',
#    'elf_params__4_28_0__20162018m3_2018april2wl.txt',
#    'elf_params__4_28_1__20162018m3_2018april2wl.txt',
#    'elf_params__4_35_0__20162018m3_2018april2wl.txt',
#    'elf_params__4_35_1__20162018m3_2018april2wl.txt',
    'elf_params__4_60_0__20162018m3_2018april2wl.txt',
    'elf_params__4_60_1__20162018m3_2018april2wl.txt',
    'elf_params__5_21_0__20162018m3_2018april2wl.txt',
    'elf_params__5_21_1__20162018m3_2018april2wl.txt',
    'elf_params__5_28_0__20162018m3_2018april2wl.txt',
    'elf_params__5_28_1__20162018m3_2018april2wl.txt',
    'elf_params__5_35_0__20162018m3_2018april2wl.txt',
    'elf_params__5_35_1__20162018m3_2018april2wl.txt',
    'elf_params__5_60_0__20162018m3_2018april2wl.txt',
    'elf_params__5_60_1__20162018m3_2018april2wl.txt',
],
'SEA': ['0', '0.5', '1.0'],
'backcast_dates': ['21', '28'],
'backcast_agg_rule': ['hour,bd' ],
'bestmatch_first_guess': ['temp', 'dd'],
'backcast_error_application': ['relative', 'absolute'],
'backcast_error_source':  ['before_smooth', 'smoothed'],
'match_rule': ['DowBD'],    
'exclude_today_from_match': [],
'round_temp_for_miss': []    
}

def variable_param_values(paramgrid):
    def gen_param_values():
        '''
        returns two values: 
            dictionary of variable parameters set
            array of parameters        
        '''
        total_permutations = 1
        for param, values in paramgrid.items():
            l = len(values)
            if (l==0):
                l = 2                
            total_permutations = total_permutations * l
        for num in range(total_permutations):
            result_dict = {}
            result_array = []
            result_dir = ''
            iteration_val = num
            for param, values in paramgrid.items():
                avail_values = values
                is_option = len(avail_values) == 0
                if (is_option):
                    avail_values = [0, 1]
                dim = len(avail_values)
                current_option_no = iteration_val % dim
                iteration_val = iteration_val // dim
                param_value = avail_values[current_option_no]
                if (not is_option) or param_value==1:
                    result_array.append('-' + param)
                if (not is_option): 
                    result_array.append(str(param_value))
                result_dict[param] = str(param_value)
                if (len(result_dir)):
                    result_dir = result_dir + '__'
                if (is_option):
                    result_dir = result_dir + param
                else:
                    result_dir = result_dir + param_value
                result_dir = result_dir.replace(',','')
            yield (result_dict, result_array, result_dir)
    return gen_param_values


exefile = 'C:/development/projects/ansergy/elf/build/bin/elf.exe'
workdir = 'C:/development/projects/ansergy/elf_new_data' 
outdir = 'D:/development/elf_calc' 

common_params = ['-orgcodes', 'elf_orgcodes_part.txt', 
                 '-dd_adj', 'elf_ddadd.txt', 
                 '-forecast', 'elf_tempfc_2018.txt', 
                 '-history', 'elf_history.txt',
                 '-default_missing_dd_adj_to_0', '-ignore_missing_utility_data']

output_params = { 'output_utility': 'utility.out', 
                  'output_backcast_diff': 'backcast_diff.out',
                  'output_backcast_agg': 'backcast_agg.out',
                  'output_backcast_utility': 'backcast_utility.out' }


def calc_elf_grid_param_stats(exefile, work_dir, out_dir, paramgrid, common_params, output_params, fileout_stat):
    '''
    paramgrid - dictionary with parameter possible values 
    common_params - array with common static parameters
    output_params - dictionary with parameters
    '''                
    params = common_params
    paramgrid_index = []
    gen = variable_param_values(paramgrid)
    output_cols = list(paramgrid.keys())
    output_cols.extend(['avg_abs_error', 'max_abs_error', 'avg_error'])
    output_cols.extend(['avg_abs_agg_backcast_pct_error', 'max_abs_agg_backcast_pct_error', 'avg_agg_backcast_nsize', 'min_agg_backcast_nsize'])
    result_frame = pd.DataFrame(columns=output_cols)
    fstat_name = os.path.join(outdir, fileout_stat)
    row = 0
    def generate_out_params_list(out_dir, output_params):
        result = []
        for key, value in output_params.items():
            result.append('-' + key)
            result.append(os.path.join(out_dir, value))
        return result
    gridsize = param_grid_size(paramgrid)
    print (f'getting ready to make {gridsize} calculations')
    for paramdict, paramarray, paramdir in gen():
        current_outdir = os.path.join(outdir, paramdir)
        if not os.path.exists(current_outdir):
            os.makedirs(current_outdir)
        current_output_filename = os.path.join(current_outdir, 'output.log')
        foutput = open(current_output_filename,'w+')
        subprocess_params = [exefile]
        subprocess_params.extend(common_params)
        subprocess_params.extend(paramarray)
        subprocess_params.extend(generate_out_params_list(current_outdir, output_params))
        subprocess.run(subprocess_params, cwd = work_dir, stdout = foutput)
        f_output_utility = os.path.join(current_outdir, 'utility.out')
        avg_abs_error, max_abs_error, avg_error, df = mcalculate_error(f_output_utility, 'sload')
        paramdict['avg_abs_error'] = avg_abs_error
        paramdict['max_abs_error'] = max_abs_error
        paramdict['avg_error'] = avg_error
        f_output_backcast_agg_error = os.path.join(current_outdir, 'backcast_agg.out')
        df_backcast_agg_error = pd.read_csv(f_output_backcast_agg_error)
        abs_backcast_agg_error = np.abs(df_backcast_agg_error["MeanLoadDiffPct"])                          
        abs_backcast_agg_error_bs = np.abs(df_backcast_agg_error["MeanLoadBeforeSmoothDiffPct"])
        
        paramdict['avg_abs_agg_backcast_pct_error'] = abs_backcast_agg_error.mean()
        paramdict['max_abs_agg_backcast_pct_error'] = abs_backcast_agg_error.max()
        paramdict['avg_abs_agg_backcast_pct_error_bs'] = abs_backcast_agg_error_bs.mean()
        paramdict['max_abs_agg_backcast_pct_error_bs'] = abs_backcast_agg_error_bs.max()

        paramdict['avg_agg_backcast_nsize'] = df_backcast_agg_error['nsize'].mean()
        paramdict['min_agg_backcast_nsize'] = df_backcast_agg_error['nsize'].min()

        for key, value in paramdict.items():
            result_frame.loc[row, key] = value
        row = row + 1
        result_frame.to_csv(fstat_name)
        foutput.close()
        print ('calculated ' + str(row))
    return result_frame        

d = calc_elf_grid_param_stats(exefile, workdir, outdir, varying_params_2w, common_params, output_params,'stats_2w_april2wl_2.csv')

