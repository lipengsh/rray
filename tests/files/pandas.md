```python
import datetime
import numpy as np
# import cudf
import time
from numba import cuda
import pandas as pd
import pynvml
import logging
from line_profiler import LineProfiler

logging.basicConfig(level=logging.INFO, format='%(asctime)s-%(name)s-%(message)s')

# from brison_cal.data_prepare import brinson_prepare
# from brison_cal.aggregate_dimension import aggregate_dimension
# from brison_cal.aggregation_time import aggregate_time_main
# from brison_cal.get_options import yaml_data
# %run benchmark_data.py
%run data_prepare.py
%run aggregate_dimension.py
%run aggregation_time.py
%run get_options.py
# %run benchmark_data_new.py

def aggre_main(concat_df, absBP, dimension_list_d, brinson_cal_tag, return_bm_df):
    useful_brinson_df = aggregate_dimension(concat_df, absBP, dimension_list_d, brinson_cal_tag,return_bm_df)
    logging.info("aggregate_dimension  total --- meminfo: {}".format(
        str(pynvml.nvmlDeviceGetMemoryInfo(handle).used / 1024 / 1024)))
    return useful_brinson_df


def brinson_main(cmbno, start, end, dimension_list, bm_code, is_pene, table_only):
    brinson_df, return_bm_df, dimension_list_d, brinson_cal_tag, datecount,absBP = brinson_prepare(cmbno, start, end, dimension_list,
                                                                                      bm_code, is_pene, yaml_data)
    print(dimension_list_d)
    dimension_list = dimension_list_d.copy()
    
    useful_brinson_df = aggre_main(brinson_df, absBP, dimension_list_d, brinson_cal_tag,return_bm_df)
    result_df, result_df_date_cumsum = aggregate_time_main(useful_brinson_df, absBP, dimension_list, datecount,
                                                           brinson_cal_tag, table_only)
    logging.info("complete all, result len --- meminfo: {}".format(
        str(pynvml.nvmlDeviceGetMemoryInfo(handle).used / 1024 / 1024)))
    # result_df.to_csv('result_df.csv')
    # result_df_date_cumsum.to_csv('result_df_date_cumsum.csv')
    return result_df, result_df_date_cumsum


    # cmbno = 'F8388'
cmbno = 'N00001017'
start = '2019-01-01'
end = '2019-12-31'
dimension_list = ['asset_cls', 'shenwan1','cb_rating','symbol']
#dimension_list = ['asset_cls','symbol']
#bm_code = 'BM00003235'# 股票+债券
#bm_code = 'BM00002975' #股票+现金
#bm_code = 'BM00001845' # absBP，现金
# bm_code = '000001'
bm_code = 'ZD0020'
is_pene = 1
table_only = False
#     cmbno = 'F0102'
#     start = '2019-2-22'
#     end = '2019-03-31'
#     dimension_list = ['asset_cls','shenwan1','symbol']
#     bm_dict = 'BM00002822'
    
#     t_start = time.time()
#     brinson_df, return_bm_df, dimension_list_d, brinson_cal_tag, datecount,absBP = brinson_prepare(cmbno, start, end, dimension_list,
#                                                                                       bm_code, is_pene, yaml_data)
# #     print(dimension_list_d)
#     dimension_list = dimension_list_d.copy()
#     useful_brinson_df = aggre_main(brinson_df, absBP, dimension_list_d, brinson_cal_tag,return_bm_df)
#     result_df, result_df_date_cumsum = aggregate_time_main(useful_brinson_df, absBP, dimension_list, datecount,
#                                                            brinson_cal_tag, table_only)
#     t_end = time.time()

#     print('time used: %s' % (t_end - t_start))
    
#     # result_df, result_df_date_cumsum = brinson_main(cmbno, start, end, dimension_list, bm_dict, is_pene)
lp = LineProfiler()
lp.add_function(aggregate_time_main)
lp.add_function(brinson_prepare)
lp.add_function(merge_all_df)
lp.add_function(aggregate_dimension)
lp_wrapper = lp(brinson_main)
lp_wrapper(cmbno, start, end, dimension_list, bm_code, is_pene, table_only)
lp.print_stats()

# %timeit result_df, result_df_date_cumsum = brinson_main(cmbno, start, end, dimension_list, bm_code, is_pene, table_only)
```

    2020-04-09 02:24:55,210-root-get yaml file data
    /rapids/notebooks/utils/chengxi/get_options.py:16: YAMLLoadWarning: calling yaml.load() without Loader=... is deprecated, as the default Loader is unsafe. Please read https://msg.pyyaml.org/load for full details.
      data = yaml.load(file_data)
    2020-04-09 02:24:55,217-root-yaml data load success!
    2020-04-09 02:24:55,332-root-Finish benchmark table~
    2020-04-09 02:25:14,703-root-Finish bm_weight cal~


    7413964
    1120258
    2019-12-31 00:00:00
    2019-12-31 00:00:00


    2020-04-09 02:25:23,827-root-bm_weight, holding_df merge 
    2020-04-09 02:25:55,674-root-add industry  complete
    2020-04-09 02:26:17,234-root-add rate complete
    2020-04-09 02:26:39,671-root-fill na 
    2020-04-09 02:26:40,526-root-add absBP complete
    2020-04-09 02:26:41,173-root-add security_name 


    ['asset_cls', '申万一级', 'cb_rating', 'symbol']


    2020-04-09 02:26:42,397-root-aggregate_dimension
    /rapids/notebooks/utils/chengxi/aggregate_dimension.py:62: SettingWithCopyWarning: 
    A value is trying to be set on a copy of a slice from a DataFrame.
    Try using .loc[row_indexer,col_indexer] = value instead
    
    See the caveats in the documentation: http://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
      sub_brinson['return_ptf'] = 0.0
    2020-04-09 02:26:48,921-root-aggregate_dimension --- sub_brinson upgrade
    2020-04-09 02:27:04,702-root-aggregate_dimension --- create_df_dimension
    2020-04-09 02:27:11,249-root-aggregate_dimension --- slice.groupby
    2020-04-09 02:27:14,159-root-aggregate_dimension --- slice.sum
    2020-04-09 02:27:14,220-root-aggregate_dimension --- sub_brinson upgrade
    2020-04-09 02:27:14,269-root-aggregate_dimension --- merge
    2020-04-09 02:27:14,288-root-aggregate_dimension --- BHB BF
    2020-04-09 02:27:14,394-root-aggregate_dimension --- create_df_dimension
    2020-04-09 02:27:14,461-root-aggregate_dimension --- slice.groupby
    2020-04-09 02:27:14,508-root-aggregate_dimension --- slice.sum
    2020-04-09 02:27:14,542-root-aggregate_dimension --- sub_brinson upgrade
    2020-04-09 02:27:14,565-root-aggregate_dimension --- create_df_dimension
    2020-04-09 02:27:14,585-root-aggregate_dimension --- slice.groupby
    2020-04-09 02:27:14,599-root-aggregate_dimension --- slice.sum
    2020-04-09 02:27:14,628-root-aggregate_dimension --- sub_brinson upgrade
    2020-04-09 02:27:14,639-root-aggregate_dimension --- create_df_dimension
    2020-04-09 02:27:14,653-root-aggregate_dimension --- slice.groupby
    2020-04-09 02:27:14,660-root-aggregate_dimension --- slice.sum
    2020-04-09 02:27:15,100-root-aggregate_dimension --- sub_brinson upgrade
    /rapids/notebooks/utils/chengxi/aggregate_dimension.py:138: FutureWarning: Sorting because non-concatenation axis is not aligned. A future version
    of pandas will change to not sort by default.
    
    To accept the future behavior, pass 'sort=False'.
    
    To retain the current behavior and silence the warning, pass 'sort=True'.
    
      useful_brinson_df = pd.concat(useful_brinson_df_list, ignore_index=True, axis=0)
    2020-04-09 02:27:20,097-root-aggregate_dimension --- concat,useful_brinson_df
    2020-04-09 02:27:31,770-root-aggregate_dimension  total --- meminfo: 1042.875
    2020-04-09 02:27:40,765-root-aggregate_time dimension_aggr_df merge return_df
    2020-04-09 02:27:51,152-root-aggregate_time ---result_df_cumprod2 groupby sum
    2020-04-09 02:28:02,758-root-aggregate_time result_df_cumsum
    2020-04-09 02:28:08,385-root-aggregation_main ---get_result_df_date_cumsum cost time
    2020-04-09 02:28:08,390-root-complete all, result len --- meminfo: 1042.875


    Timer unit: 1e-06 s
    
    Total time: 49.5781 s
    File: /rapids/notebooks/utils/chengxi/aggregate_dimension.py
    Function: aggregate_dimension at line 19
    
    Line #      Hits         Time  Per Hit   % Time  Line Contents
    ==============================================================
        19                                           def aggregate_dimension(brinson_df, absBP, dimension_list, brinson_cal_tag, return_bm_df):
        20                                               """对传入的参数进行维度聚合
        21                                           
        22                                               params:
        23                                                   brinson_df: Dataframe 待聚合数据
        24                                                   dimension: Str 维度信息
        25                                           
        26                                               Returns:
        27                                                   useful_brinson_df: Dataframe 维度聚合后的数据
        28                                               """
        29                                           
        30         1          3.0      3.0      0.0      dimension_list_d = dimension_list.copy()
        31         1          1.0      1.0      0.0      dimension_sum_list = ['weight', 'weight_bm']
        32         1          2.0      2.0      0.0      dimension_multiply_list = ['return_ptf', 'return_mkt']
        33         1          2.0      2.0      0.0      len_dimension_list = len(dimension_list)
        34         1          3.0      3.0      0.0      groupkey = ['the_date'] + dimension_list
        35         1     726638.0 726638.0      1.5      useful_brinson_df = brinson_df[dimension_sum_list + dimension_multiply_list + groupkey]
        36                                               # 最底层个券tag为1
        37                                               # useful_brinson_df['tag'] = -1
        38         1          3.0      3.0      0.0      useful_brinson_df_list = [useful_brinson_df]
        39         1        952.0    952.0      0.0      logging.info("aggregate_dimension")
        40         6         16.0      2.7      0.0      for i in range(len_dimension_list + 1):
        41                                                   # 把上一层聚合的结果拿出来
        42                                                   # slice = useful_brinson_df[useful_brinson_df.tag == i - 1].copy()
        43         5         17.0      3.4      0.0          slice = useful_brinson_df_list[len(useful_brinson_df_list) - 1]
        44         5         55.0     11.0      0.0          if len(slice) > 0:
        45                                                       # 每一层按照return_ptf和return_mkt的weight加权求和
        46         5     311292.0  62258.4      0.6              slice['return_ptf_w'] = slice['return_ptf'] * slice['weight']
        47         5     308756.0  61751.2      0.6              slice['return_mkt_w'] = slice['return_mkt'] * slice['weight_bm']
        48                                           
        49                                                       ## groupkey中有symbol时，sum之后就是原始df
        50         5         27.0      5.4      0.0              if 'symbol' in groupkey:
        51         1    1575247.0 1575247.0      3.2                  sub_brinson = slice[['return_ptf_w', 'return_mkt_w'] + dimension_sum_list + groupkey]
        52                                                       else:
        53         4    4624309.0 1156077.2      9.3                  sub_brinson = slice[['return_ptf_w', 'return_mkt_w'] + dimension_sum_list + groupkey].groupby(groupkey,
        54         4     221368.0  55342.0      0.4                                                                                                                as_index=False)
        55                                                           # sub_brinson = slice[['return_ptf_w','return_mkt_w'] + dimension_sum_list + groupkey]
        56         4       4592.0   1148.0      0.0                  logging.info("aggregate_dimension --- slice.groupby")
        57                                           
        58         4    2973376.0 743344.0      6.0                  sub_brinson = sub_brinson.sum()
        59                                           
        60         4       4680.0   1170.0      0.0                  logging.info("aggregate_dimension --- slice.sum")
        61                                           
        62         5      49106.0   9821.2      0.1              sub_brinson['return_ptf'] = 0.0
        63         5      47682.0   9536.4      0.1              sub_brinson['return_mkt'] = 0.0
        64         5    1898997.0 379799.4      3.8              sub_brinson = sub_brinson.reset_index()
        65                                                       # ---------------------------------------------------------------------------
        66         5        124.0     24.8      0.0              if len(sub_brinson) == 1:
        67                                                           sub_brinson.loc[0, 'return_ptf'] = sub_brinson.loc[0, 'return_ptf_w'].values / sub_brinson.loc[
        68                                                               0, 'weight'].values if \
        69                                                               list(sub_brinson.loc[0, 'weight'])[0] != 0.0 else 0.0
        70                                                           sub_brinson.loc[0, 'return_mkt'] = sub_brinson.loc[0, 'return_mkt_w'].values / sub_brinson.loc[
        71                                                               0, 'weight_bm'].values if \
        72                                                               list(sub_brinson.loc[0, 'weight_bm'])[0] != 0.0 else 0.0
        73                                                       else:
        74         5      25043.0   5008.6      0.1                  sub_brinson['weight'].fillna(0, inplace=True)
        75         5      24836.0   4967.2      0.1                  sub_brinson['weight_bm'].fillna(0, inplace=True)
        76         5     327301.0  65460.2      0.7                  if len(sub_brinson.loc[sub_brinson.weight != 0.0]) == 1:
        77                                                               index = sub_brinson.loc[sub_brinson.weight != 0.0].index
        78                                                               sub_brinson.loc[index, 'return_ptf'] = sub_brinson.loc[index, 'return_ptf_w'] / sub_brinson.loc[
        79                                                                   index, 'weight']
        80                                                           else:
        81         5         25.0      5.0      0.0                      sub_brinson.loc[sub_brinson.weight != 0.0, 'return_ptf'] = sub_brinson.loc[
        82         5      49330.0   9866.0      0.1                                                                                     sub_brinson.weight != 0.0, 'return_ptf_w'] / \
        83         5         19.0      3.8      0.0                                                                                 sub_brinson.loc[
        84         5     692146.0 138429.2      1.4                                                                                     sub_brinson.weight != 0.0, 'weight']
        85         5    1253971.0 250794.2      2.5                  if len(sub_brinson.loc[sub_brinson.weight_bm != 0.0]) == 1:
        86                                                               index = sub_brinson.loc[sub_brinson.weight_bm != 0.0].index
        87                                                               sub_brinson.loc[index, 'return_mkt'] = sub_brinson.loc[index, 'return_mkt_w'] / sub_brinson.loc[
        88                                                                   index, 'weight_bm']
        89                                                           else:
        90         5         27.0      5.4      0.0                      sub_brinson.loc[sub_brinson.weight_bm != 0.0, 'return_mkt'] = sub_brinson.loc[
        91         5     175927.0  35185.4      0.4                                                                                        sub_brinson.weight_bm != 0.0, 'return_mkt_w'] / \
        92         5         21.0      4.2      0.0                                                                                    sub_brinson.loc[
        93         5     720382.0 144076.4      1.5                                                                                        sub_brinson.weight_bm != 0.0, 'weight_bm']
        94         5       6072.0   1214.4      0.0              logging.info("aggregate_dimension --- sub_brinson upgrade")
        95         5      47407.0   9481.4      0.1              sub_brinson['tag'] = i
        96                                                       # --- Brinson计算 ----
        97                                           
        98         5         13.0      2.6      0.0              if i == brinson_cal_tag:
        99                                                           # return_bm_dict = dict(zip(return_bm_df['the_date'], return_bm_df['return_bm']))
       100                                                           # sub_brinson['return_bm'] = sub_brinson['the_date'].map(lambda x: return_bm_dict[x])
       101         1          6.0      6.0      0.0                  if len(return_bm_df) > 0:
       102         1      17911.0  17911.0      0.0                      sub_brinson['the_date'] = sub_brinson['the_date'].astype('datetime64[ms]')
       103         1        925.0    925.0      0.0                      return_bm_df['the_date'] = return_bm_df['the_date'].astype('datetime64[ms]')
       104         1      27496.0  27496.0      0.1                      sub_brinson = sub_brinson.merge(return_bm_df, how='left', on='the_date')
       105                                                           else:
       106                                                               sub_brinson['return_bm'] = 0.0
       107         1       1342.0   1342.0      0.0                  logging.info("aggregate_dimension --- merge")
       108                                                           # BHB
       109         1       3463.0   3463.0      0.0                  sub_brinson['bhb_ar'] = (sub_brinson['weight'] - sub_brinson['weight_bm']) * sub_brinson['return_mkt']
       110         1        192.0    192.0      0.0                  sub_brinson['bhb_sr'] = sub_brinson['weight_bm'] * (
       111         1       2881.0   2881.0      0.0                          sub_brinson['return_ptf'] - sub_brinson['return_mkt'])
       112         1        904.0    904.0      0.0                  sub_brinson['bhb_ir'] = (sub_brinson['weight'] - sub_brinson['weight_bm']) * \
       113         1       2878.0   2878.0      0.0                                          (sub_brinson['return_ptf'] - sub_brinson['return_mkt'])
       114                                           
       115                                                           # BF
       116         1        894.0    894.0      0.0                  sub_brinson['bf_ar'] = (sub_brinson['weight'] - sub_brinson['weight_bm']) * \
       117         1       2865.0   2865.0      0.0                                         (sub_brinson['return_mkt'] - sub_brinson['return_bm'])
       118         1       2886.0   2886.0      0.0                  sub_brinson['bf_sr'] = sub_brinson['weight'] * (sub_brinson['return_ptf'] - sub_brinson['return_mkt'])
       119         1       1192.0   1192.0      0.0                  logging.info("aggregate_dimension --- BHB BF")
       120                                                       else:
       121         4      45874.0  11468.5      0.1                  sub_brinson['bhb_ar'] = 0.0
       122         4      62380.0  15595.0      0.1                  sub_brinson['bhb_sr'] = 0.0
       123         4      64773.0  16193.2      0.1                  sub_brinson['bhb_ir'] = 0.0
       124         4      60638.0  15159.5      0.1                  sub_brinson['bf_ar'] = 0.0
       125         4      58743.0  14685.8      0.1                  sub_brinson['bf_sr'] = 0.0
       126                                                       # groupkey去掉最后一个，进入下一层循环
       127         5         18.0      3.6      0.0              if len(groupkey) > 1:
       128         4   15590085.0 3897521.2     31.4                  sub_brinson = create_df_dimension(dimension_list, sub_brinson)
       129         4         17.0      4.2      0.0                  dimension_list.pop()
       130                                                       else:
       131         1       1194.0   1194.0      0.0                  sub_brinson['dimension'] = '合计'
       132         5     245283.0  49056.6      0.5              sub_brinson['label'] = sub_brinson[groupkey.pop()] if len(groupkey) > 1 else '合计'
       133         5    1176292.0 235258.4      2.4              sub_brinson['the_date'] = sub_brinson['the_date'].astype('datetime64[ms]')
       134         5         24.0      4.8      0.0              useful_brinson_df_list.append(sub_brinson)
       135                                                       # useful_brinson_df = sub_brinson if i == 0 else cudf.concat([useful_brinson_df, sub_brinson],ignore_index=True, axis=0)
       136                                               # 除去原始数据，只保留聚合结果
       137         1          2.0      2.0      0.0      del useful_brinson_df_list[0]
       138         1    4984993.0 4984993.0     10.1      useful_brinson_df = pd.concat(useful_brinson_df_list, ignore_index=True, axis=0)
       139         1       1504.0   1504.0      0.0      logging.info("aggregate_dimension --- concat,useful_brinson_df")
       140                                               # useful_brinson_df = useful_brinson_df[useful_brinson_df.tag > -1].reset_index(drop=True, inplace=False)
       141         1    6663572.0 6663572.0     13.4      useful_brinson_df = useful_brinson_df.reset_index(drop=True, inplace=False)
       142                                               # 用label字段表示层级
       143                                               # useful_brinson_df = create_df_dimension(dimension_list,useful_brinson_df)
       144         1     751781.0 751781.0      1.5      del useful_brinson_df['return_ptf_w']
       145         1     690910.0 690910.0      1.4      del useful_brinson_df['return_mkt_w']
       146         5         18.0      3.6      0.0      for dimension in dimension_list_d:
       147         4          7.0      1.8      0.0          if dimension != 'symbol':
       148         3    3044355.0 1014785.0      6.1              del useful_brinson_df[dimension]
       149         1          2.0      2.0      0.0      return useful_brinson_df
    
    Total time: 36.6159 s
    File: /rapids/notebooks/utils/chengxi/aggregation_time.py
    Function: aggregate_time_main at line 205
    
    Line #      Hits         Time  Per Hit   % Time  Line Contents
    ==============================================================
       205                                           def aggregate_time_main(dimension_aggr_df, absBP, dimension_list, datecount, brinson_cal_tag, table_only):
       206                                               # -------------------------- 串行计算 start ---------------------
       207                                               # 时间上聚合
       208         1         17.0     17.0      0.0      if len(dimension_aggr_df) == 0:
       209                                                   return pd.DataFrame(columns=result_list), pd.DataFrame(
       210                                                       columns=['yieldContributionOfCombination', 'yieldContributionOfStandard', 'returnAllocationBHB',
       211                                                                'returnSelectionBHB', 'returnInteractionBHB', 'returnAllocationBF', 'returnSelectionBF',
       212                                                                'yieldContributionOfOverweight', 'tradingDay'])
       213         1   36428304.0 36428304.0     99.5      result_df, dimension_aggr_brinsontag = aggregate_time(dimension_aggr_df, dimension_list, datecount, brinson_cal_tag, table_only)
       214                                           
       215                                               # 绝对BP补丁
       216         1          3.0      3.0      0.0      if absBP and absBP != 0:
       217                                                   result_df, dimension_aggr_brinsontag = patch_absBP(result_df, dimension_aggr_brinsontag, absBP, datecount,
       218                                                                                                      brinson_cal_tag)
       219                                           
       220         1       7945.0   7945.0      0.0      result_df = result_df[result_list]
       221         1          2.0      2.0      0.0      if table_only:
       222                                                   result_df_date_cumsum = pd.DataFrame()
       223                                               else:
       224         1     178211.0 178211.0      0.5          result_df_date_cumsum = get_result_df_date_cumsum(dimension_aggr_brinsontag)
       225         1       1412.0   1412.0      0.0      logging.info('aggregation_main ---get_result_df_date_cumsum cost time')
       226         1          1.0      1.0      0.0      return result_df, result_df_date_cumsum
    
    Total time: 86.2839 s
    File: /rapids/notebooks/utils/chengxi/data_prepare.py
    Function: merge_all_df at line 35
    
    Line #      Hits         Time  Per Hit   % Time  Line Contents
    ==============================================================
        35                                           def merge_all_df(cmbno, bm_df, bm_weight, date_range, is_pene, dimension_list, bm_code, output_col, absBP,
        36                                                            sector_df_list, yaml_data, mom_strats, pene):
        37         1     256633.0 256633.0      0.3      holding_df = pd.read_parquet(yaml_data['parquet_path']['holding_df'] + '/%s_%s.parquet' % (cmbno, pene))
        38         1     120688.0 120688.0      0.1      holding_df = holding_df[(holding_df['the_date'] >= date_range[0]) & (holding_df['the_date'] <= date_range[1])]
        39         1      34351.0  34351.0      0.0      del holding_df['return_mkt']
        40         1      11518.0  11518.0      0.0      momstrats_return = pd.read_parquet(yaml_data['parquet_path']['momstrats_return'])
        41         1          3.0      3.0      0.0      momstrats_return = momstrats_return[
        42         1       4131.0   4131.0      0.0          (momstrats_return['date_day'] >= date_range[0]) & (momstrats_return['date_day'] <= date_range[1])]
        43         1       6305.0   6305.0      0.0      momstrats_return = momstrats_return.drop_duplicates()
        44         1       7606.0   7606.0      0.0      submom_strats = pd.read_parquet(yaml_data['parquet_path']['submom_strats'])
        45         1       8055.0   8055.0      0.0      mom_index = pd.read_parquet(yaml_data['parquet_path']['mom_index'])
        46         1       2197.0   2197.0      0.0      mom_index = mom_index[mom_index['cmbno'] == cmbno]
        47         1       2267.0   2267.0      0.0      mom_index = mom_index[['first_strategy', 'second_strategy', 'strats_weight']]
        48         1          3.0      3.0      0.0      if mom_strats == 0:
        49         1       6067.0   6067.0      0.0          submom_strats = pd.DataFrame(columns=submom_strats.columns)
        50         1       5481.0   5481.0      0.0          mom_index = pd.DataFrame(columns=mom_index.columns)
        51         1       6162.0   6162.0      0.0          momstrats_return = pd.DataFrame(columns=momstrats_return.columns)
        52         1        167.0    167.0      0.0      print(len(bm_weight))
        53         1         53.0     53.0      0.0      print(len(holding_df))
        54         1      21887.0  21887.0      0.0      print(bm_weight.the_date.max())
        55         1       6647.0   6647.0      0.0      print(holding_df.the_date.max())
        56         1          4.0      4.0      0.0      if mom_strats == 0 or pene == 1:
        57         1     312920.0 312920.0      0.4          bm_weight['symbol'] = bm_weight['symbol'].astype('object')
        58         1         26.0     26.0      0.0          if len(bm_weight) == 0:
        59                                                       holding_df['weight_bm'] = 0
        60                                                       holding_df['return_mkt'] = 0
        61                                                       holding_df['asset_cls_bk'] = '未定义'
        62                                                   else:
        63         1    1047133.0 1047133.0      1.2              bm_weight['the_date'] = bm_weight['the_date'].astype('datetime64[ms]')
        64         1     136091.0 136091.0      0.2              holding_df['the_date'] = holding_df['the_date'].astype('datetime64[ms]')
        65         1    6941524.0 6941524.0      8.0              holding_df = pd.merge(bm_weight, holding_df, how='outer', on=['the_date', 'symbol'])  # modify
        66         1       1341.0   1341.0      0.0          logging.info("bm_weight, holding_df merge ")
        67                                                   # return contribution of benchmark components
        68         1     408567.0 408567.0      0.5          return_bm = bm_weight.copy()
        69                                               #     return_bm['return_bm'] = return_bm['return_mkt_bm'] * return_bm['weight_bm']
        70                                               #     return_bm['return_bm'].fillna(0, inplace=True)
        71                                           
        72                                               # merge basic info
        73         1     772462.0 772462.0      0.9      sym_list = holding_df['symbol'].unique().tolist()
        74         1     595679.0 595679.0      0.7      basic_info = pd.read_parquet(yaml_data['parquet_path']['basic_info'])
        75         1      73219.0  73219.0      0.1      basic_info = basic_info.loc[basic_info.symbol.isin(sym_list)][['symbol', 'code_tier3', 'security_name']]
        76         1    2909280.0 2909280.0      3.4      holding_df.rename(columns={'code_tier3': 'code_tier3_info'}, inplace=True)
        77         1    5952137.0 5952137.0      6.9      holding_df = pd.merge(holding_df, basic_info, on='symbol', how='left')
        78         1    3095237.0 3095237.0      3.6      holding_df.loc[holding_df['code_tier3'].isnull(), 'code_tier3'] = holding_df.loc[holding_df['code_tier3'].isnull()][
        79         1     620091.0 620091.0      0.7          'code_tier3_info']
        80         1     763629.0 763629.0      0.9      del holding_df['code_tier3_info']
        81         1     597232.0 597232.0      0.7      holding_df['code_tier3'] = holding_df['code_tier3'].fillna('')
        82                                               # merge asset class
        83         1       8103.0   8103.0      0.0      asset_class_df = pd.read_parquet(yaml_data['parquet_path']['asset_class_df'])
        84         1     347618.0 347618.0      0.4      code_list = holding_df['code_tier3'].unique().tolist()
        85         1       1965.0   1965.0      0.0      asset_class_df = asset_class_df.loc[asset_class_df.code_tier3.isin(code_list)]
        86         1    6171569.0 6171569.0      7.2      holding_df = holding_df.merge(asset_class_df, on='code_tier3', how='left')
        87         1          5.0      5.0      0.0      if mom_strats == 0 or pene == 1:
        88         1     756338.0 756338.0      0.9          holding_df['asset_cls'] = holding_df['asset_cls'].fillna(holding_df['asset_cls_bk'])
        89         1     138300.0 138300.0      0.2          del holding_df['asset_cls_bk']
        90                                               # 合并策略
        91         1          4.0      4.0      0.0      if mom_strats == 1 and pene == 0:
        92                                                   holding_df = pd.merge(holding_df, submom_strats[['symbol', 'first_strategy', 'second_strategy']], how='left',
        93                                                                           on='symbol')
        94                                                   holding_df[['first_strategy', 'second_strategy']] = holding_df[['first_strategy', 'second_strategy']].fillna(
        95                                                       "未定义")
        96                                                   logging.info("first_strategy")
        97                                                   # 如果是大mom，并且按照策略指数归因
        98                                                   # if mom_strats == 1 and pene == 0:
        99                                                   return_bm = pd.merge(momstrats_return, mom_index, how='inner', on=['first_strategy', 'second_strategy'])
       100                                                   ###### 当 holding_df 有多个子组合属于同一个基准时，需要将strats_weight按照子组合个数调整 ####
       101                                                   cnt_adj = holding_df.groupby(['the_date', 'first_strategy', 'second_strategy']).count()['symbol'].reset_index()
       102                                                   cnt_adj = cnt_adj.rename(columns={'symbol': 'symbol_cnt', 'the_date': 'date_day'})
       103                                                   cnt_adj['date_day'] = cnt_adj['date_day'].astype('datetime64[ms]')
       104                                                   return_bm['date_day'] = return_bm['date_day'].astype('datetime64[ms]')
       105                                                   return_bm = return_bm.merge(cnt_adj, how='left', on=['date_day', 'first_strategy', 'second_strategy'])
       106                                                   return_bm['symbol_cnt'] = return_bm['symbol_cnt'].fillna(1)
       107                                                   return_bm['strats_weight'] = return_bm['strats_weight'] / return_bm['symbol_cnt']
       108                                                   del return_bm['symbol_cnt']
       109                                                   ###################################################################################
       110                                                   return_bm.rename(
       111                                                       columns={'daily_weekly_rate': 'return_mkt', 'strats_weight': 'weight_bm', 'date_day': 'the_date'},
       112                                                       inplace=True)
       113                                                   return_bm['st_symbol'] = return_bm['first_strategy'] + return_bm['second_strategy']
       114                                                   holding_df['the_date'] = holding_df['the_date'].astype('datetime64[ms]')
       115                                                   return_bm['the_date'] = return_bm['the_date'].astype('datetime64[ms]')
       116                                                   holding_df = pd.merge(holding_df, return_bm, how='outer',
       117                                                                           on=['the_date', 'first_strategy', 'second_strategy']).sort_values(
       118                                                       'the_date').reset_index(drop=True)  # modify
       119                                                   ################################################
       120                                                   ## outer join 之后把字段赋值
       121                                                   #     holding_df['weight_bm']= holding_df['weight_bm'].fillna(0)
       122                                                   #     holding_df['weight_e']= holding_df['weight_e'].fillna(0)
       123                                                   #     holding_df['weight']= holding_df['weight'].fillna(0)
       124                                                   #     holding_df['return_ptf']= holding_df['return_ptf'].fillna(0)
       125                                                   ################################################
       126                                                   holding_df.loc[~holding_df['st_symbol'].isna(), 'asset_cls'] = '权益'
       127                                                   holding_df['symbol'] = holding_df['symbol'].fillna(holding_df['st_symbol'])
       128                                                   del holding_df['end_weekly_rate']
       129                                                   logging.info("mom  complete")
       130                                               else:
       131         1      74807.0  74807.0      0.1          holding_df['first_strategy'] = "未定义"
       132         1      75141.0  75141.0      0.1          holding_df['second_strategy'] = "未定义"
       133                                               # 合并基准收益
       134         1     123657.0 123657.0      0.1      return_bm['return_bm'] = return_bm['return_mkt'] * return_bm['weight_bm']
       135         1      19605.0  19605.0      0.0      return_bm['return_bm'].fillna(0, inplace=True)
       136         1    1340934.0 1340934.0      1.6      return_bm_df = return_bm[['the_date', 'return_bm']].sort_values('the_date').groupby(['the_date'],
       137         1     505891.0 505891.0      0.6                                                                                          as_index=False).sum()
       138                                           
       139                                               # 行业数据合并
       140         1     763790.0 763790.0      0.9      sym_list = holding_df['symbol'].unique().tolist()
       141         1         18.0     18.0      0.0      if list(set(sector_df_list).intersection(set(dimension_list))):
       142         1          5.0      5.0      0.0          sector = list(set(sector_df_list).intersection(set(dimension_list)))[0]
       143         1      11293.0  11293.0      0.0          sector_type = pd.read_parquet(yaml_data['parquet_path']['sector_list'])
       144         1          7.0      7.0      0.0          sector_df = pd.read_parquet(yaml_data['parquet_path']['sector_df'] + '%s.parquet' % (
       145         1     109531.0 109531.0      0.1              sector_type.loc[sector_type.index == sector]['industry_type'].iloc[0]))
       146         1       5420.0   5420.0      0.0          sector_df.rename(columns={'industry': sector}, inplace=True)
       147         1      37078.0  37078.0      0.0          sector_df = sector_df[sector_df.symbol.isin(sym_list)]
       148         1       2486.0   2486.0      0.0          sector_df = sector_df[['symbol'] + list(set(sector_df_list).intersection(set(dimension_list)))]
       149                                           
       150         1         36.0     36.0      0.0          if sector_df.empty:
       151                                                       holding_df[sector] = '未定义'
       152                                                   else:
       153         1    5563960.0 5563960.0      6.4              holding_df = pd.merge(holding_df, sector_df, on='symbol', how='left')
       154         1       4578.0   4578.0      0.0          logging.info("add industry  complete")
       155                                               # 中债隐含评级合并
       156         1          5.0      5.0      0.0      if 'cb_rating' in dimension_list:
       157         1      98494.0  98494.0      0.1          rate_df1 = pd.read_parquet(yaml_data['parquet_path']['rate_df'])
       158         1      41218.0  41218.0      0.0          rate_df1 = rate_df1[rate_df1.symbol.isin(sym_list)]
       159         1      11699.0  11699.0      0.0          rate_df1 = rate_df1.loc[(rate_df1.start_date <= date_range[1]) & (rate_df1.end_date >= date_range[0])]
       160         1         32.0     32.0      0.0          if rate_df1.empty:
       161                                                       holding_df['cb_rating'] = '未定义'
       162                                                   else:
       163         1    8974769.0 8974769.0     10.4              holding_df = pd.merge(holding_df, rate_df1, on=['symbol'], how='left')  # modify
       164         1      64568.0  64568.0      0.1              holding_df['flag'] = 1
       165                                                       holding_df.loc[((holding_df.the_date < holding_df.start_date) | (
       166         1    7330842.0 7330842.0      8.5                      holding_df.the_date > holding_df.end_date)), 'flag'] = 0
       167                                           
       168         1    2477081.0 2477081.0      2.9              holding_df = holding_df.loc[holding_df.flag == 1]
       169         1       8734.0   8734.0      0.0              del holding_df['flag']
       170         1    1691522.0 1691522.0      2.0              holding_df.rename(columns={'rate_result': 'cb_rating'}, inplace=True)
       171         1     157404.0 157404.0      0.2              del holding_df['start_date']
       172         1      81243.0  81243.0      0.1              del holding_df['end_date']
       173         1     617880.0 617880.0      0.7              holding_df['cb_rating'] = holding_df['cb_rating'].fillna('未定义')
       174                                           
       175         1       1447.0   1447.0      0.0          logging.info("add rate complete")
       176                                           
       177                                               # fill na at once
       178                                               holding_df.loc[(holding_df['cmbno'].isnull()) | (holding_df['return_ptf'] == float('inf')) | (
       179         1     601540.0 601540.0      0.7              holding_df['return_ptf'] == float('-inf')), 'return_ptf'] = 0
       180         1     125534.0 125534.0      0.1      holding_df.cmbno = cmbno
       181         1     764850.0 764850.0      0.9      holding_df['security_name'] = holding_df['security_name'].fillna("")
       182                                               #     holding_df['asset_cls'] = holding_df['asset_cls'].fillna("未定义")
       183         1     746388.0 746388.0      0.9      holding_df.loc[holding_df.code_tier3 == '30503', 'symbol'] += ' 利率互换'
       184         1     808304.0 808304.0      0.9      holding_df.loc[holding_df.symbol == 'DEPO_CURRENT', 'symbol'] = 'DEPO_CURRENT 活期存款'
       185         1     785285.0 785285.0      0.9      holding_df.loc[holding_df.symbol == 'DEPO_5Y', 'symbol'] = 'DEPO_5Y 5年存款'
       186         1     794703.0 794703.0      0.9      holding_df.loc[holding_df.symbol == 'DEPO_3Y', 'symbol'] = 'DEPO_3Y 3年存款'
       187         1     796566.0 796566.0      0.9      holding_df.loc[holding_df.symbol == 'DEPO_2Y', 'symbol'] = 'DEPO_2Y 2年存款'
       188         1     795341.0 795341.0      0.9      holding_df.loc[holding_df.symbol == 'DEPO_1Y', 'symbol'] = 'DEPO_1Y 1年存款'
       189         1     802246.0 802246.0      0.9      holding_df.loc[holding_df.symbol == 'DEPO_6M', 'symbol'] = 'DEPO_6M 6个月存款'
       190         1     801257.0 801257.0      0.9      holding_df.loc[holding_df.symbol == 'DEPO_3M', 'symbol'] = 'DEPO_3M 3个月存款'
       191         1     716780.0 716780.0      0.8      holding_df[['weight', 'weight_e', 'weight_bm']] = holding_df[['weight', 'weight_e', 'weight_bm']].fillna(0)
       192         1          5.0      5.0      0.0      holding_df[['return_mkt', 'weight', 'weight_e', 'weight_bm']] = holding_df[
       193         1    1013222.0 1013222.0      1.2          ['return_mkt', 'weight', 'weight_e', 'weight_bm']].fillna(0)
       194         1    9751437.0 9751437.0     11.3      holding_df = get_security_name(holding_df)
       195                                           
       196         5         39.0      7.8      0.0      for field in dimension_list:
       197         4    3131911.0 782977.8      3.6          holding_df[field] = holding_df[field].fillna('未定义')
       198                                           
       199         1       2474.0   2474.0      0.0      logging.info("fill na ")
       200                                           
       201         1          5.0      5.0      0.0      if 'symbol' in output_col:
       202         1     851952.0 851952.0      1.0          output = holding_df[output_col + ['security_name']]
       203                                               else:
       204                                                   output = holding_df[output_col + ['security_name', 'symbol']]
       205         1        110.0    110.0      0.0      output.reset_index(drop=True, inplace=True)
       206                                               #   absBP
       207         1          4.0      4.0      0.0      if absBP != 0:
       208                                                   brinson_df = append_absBP(output, dimension_list)
       209                                               else:
       210         1          3.0      3.0      0.0          brinson_df = output
       211         1       1055.0   1055.0      0.0      logging.info("add absBP complete")
       212         1     645616.0 645616.0      0.7      del brinson_df['security_name']
       213         1       1366.0   1366.0      0.0      logging.info("add security_name ")
       214                                               # brinson_df.to_csv('brinson_df_end.csv')
       215         1          4.0      4.0      0.0      return brinson_df, return_bm_df
    
    Total time: 106.268 s
    File: /rapids/notebooks/utils/chengxi/data_prepare.py
    Function: brinson_prepare at line 218
    
    Line #      Hits         Time  Per Hit   % Time  Line Contents
    ==============================================================
       218                                           def brinson_prepare(cmbno, start, end, dimension_list, bm_code, is_pene, yaml_data):
       219                                               # 所有行业列表，用来判断dimension list中有没有行业成分， 有哪些行业就取哪些行业的数据。
       220         1      72371.0  72371.0      0.1      sector_type = pd.read_parquet(yaml_data['parquet_path']['sector_list'])
       221         1         72.0     72.0      0.0      sector_df_list = list(sector_type.index)
       222                                           
       223                                               # mom data
       224         1       8333.0   8333.0      0.0      mom_df = pd.read_parquet(yaml_data['parquet_path']['mom_df'])
       225         1        328.0    328.0      0.0      pene_data = 1 if cmbno in list(mom_df['cmbno']) else 2  # pene_data = 1 具有穿透/非穿透，并且都具有数据, pene_data = 2 只具有非穿透
       226         1       1755.0   1755.0      0.0      mom_strats = 1 if cmbno in list(mom_df.loc[mom_df['mom_strats'].notnull(), 'cmbno']) else 0  # 判断是否为mom策略
       227         1          2.0      2.0      0.0      pene = 1 if pene_data == 1 and is_pene else 0
       228                                           
       229         1          7.0      7.0      0.0      dimension_list = [yaml_data['DIMENSION_NAME_DICT'].get(x, x) for x in dimension_list]
       230                                               #     output_col = yaml_data['DIMENSION_SUM_LIST'] + yaml_data['DIMENSION_MULTIPLY_LIST'] + ['the_date',
       231                                               #                                                                                            'return_bm'] + dimension_list
       232         1          3.0      3.0      0.0      output_col = yaml_data['DIMENSION_SUM_LIST'] + yaml_data['DIMENSION_MULTIPLY_LIST'] + ['the_date'] + dimension_list
       233         1       3076.0   3076.0      0.0      date_range = [datetime.datetime.strptime(start, '%Y-%m-%d'),
       234         1        105.0    105.0      0.0                    datetime.datetime.strptime(end, '%Y-%m-%d')]
       235         1        948.0    948.0      0.0      datecount = len(pd.date_range(date_range[0], date_range[1], freq='D'))
       236                                           
       237                                               # bm_df and bm_weight for non mom
       238         1          3.0      3.0      0.0      if mom_strats == 0 or pene == 1:
       239         1         67.0     67.0      0.0          if os.path.exists(yaml_data['parquet_path']['bm_weight_benchmark'] + '%s/bm_weight.parquet123' % bm_code):
       240                                                       bm_df = pd.DataFrame([])
       241                                                       bm_weight = pd.read_parquet(
       242                                                           yaml_data['parquet_path']['bm_weight_benchmark'] + '%s/bm_weight.parquet' % bm_code)
       243                                                       bm_weight = bm_weight[(bm_weight['the_date'] >= parse(start)) & (bm_weight['the_date'] <= parse(end))]
       244                                                       if os.path.exists(yaml_data['parquet_path']['abs_bp'] + '%s/absBP.parquet' % bm_code):
       245                                                           abs_bp_df = pd.read_parquet(
       246                                                               yaml_data['parquet_path']['abs_bp'] + '%s/absBP.parquet' % bm_code)
       247                                                           absBP = abs_bp_df.loc[abs_bp_df['bm_code'] == bm_code]['absBP'].iloc[0]
       248                                                           absBP = (1 + absBP) ** (datecount / 365) - 1
       249                                                       else:
       250                                                           absBP = 0
       251                                           
       252                                                   else:
       253         1      27718.0  27718.0      0.0              bm_output, absBP = get_benchmark_data(start, end, bm_code, yield_ustd=0.045)
       254         1          4.0      4.0      0.0              absBP = (1 + absBP) ** (datecount / 365) - 1
       255         1          2.0      2.0      0.0              bm_df = bm_output['bm_df']
       256         1   19555477.0 19555477.0     18.4              bm_weight = get_bm_weight(bm_output, bm_code, start, end, yaml_data)
       257                                           
       258                                               else:
       259                                                   bm_df = pd.DataFrame([])
       260                                                   bm_weight = pd.DataFrame([])
       261                                                   absBP = 0
       262         1          4.0      4.0      0.0      brinson_df, return_bm_df = merge_all_df(cmbno, bm_df, bm_weight, date_range, is_pene, dimension_list, bm_code,
       263         1   86597847.0 86597847.0     81.5                                              output_col, absBP, sector_df_list, yaml_data, mom_strats, pene)  # 总的数据聚合模块
       264         1         11.0     11.0      0.0      if "symbol" in dimension_list and len(dimension_list) > 1:
       265         1          2.0      2.0      0.0          brinson_cal_tag = 1
       266                                               else:
       267                                                   brinson_cal_tag = 0
       268         1          3.0      3.0      0.0      return brinson_df, return_bm_df, dimension_list, brinson_cal_tag, datecount, absBP
    
    Total time: 193.172 s
    File: <ipython-input-2-9eb980ee20e4>
    Function: brinson_main at line 33
    
    Line #      Hits         Time  Per Hit   % Time  Line Contents
    ==============================================================
    

