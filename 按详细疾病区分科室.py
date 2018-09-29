# -*- coding:utf-8 -*-
"""
@author:mlliu
@file: 有问必答全科室训练数据生成.py
@time: 2018/08/30
"""

# 这个是自定义函数，要在本模块中先实例化才能在计算节点函数中调用使用，
# 而本模块的其他地方可以直接调用使用
from my_package.my_model import get_time
from misc import find_longest_repeating_strings, find_common_string
import codecs

# 实例化自定义的函数，注意后面是没有括号的，否则就是直接调用得到返回值了
now = get_time.now

# 计算函数，dispy将这个函数和参数一并发送到服务器节点
# 如果函数有多个参数，需要包装程tuple格式
def compute(args):
    stopword_list, keshi_ill_list, in_file_path, long_file_path, short_file_path, log_file_path = args  # 如果函数有多个参数，需要包装程tuple格式
    import codecs, json, traceback, socket, datetime, re, pyhanlp, logging, os
    # 定义log记录
    logger = logging.getLogger(__name__)
    logger.setLevel(level=logging.DEBUG)
    handler = logging.FileHandler(log_file_path)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    logger.addHandler(handler)

    #定义科室反向查找表

    try:
        long_result_dict = {}
        short_result_dict = {}

        start_time = datetime.datetime.now()
        line_set = set()
        keshi_list = ['妇科', '妇科综合', '妇科炎症', '月经不调', '宫颈糜烂', '子宫肌瘤', '宫颈疾病', '宫外孕', '卵巢囊肿', '痛经', '外阴白斑', '更年期综合症']
        with codecs.open(in_file_path, encoding="utf8") as in_json_file:
            count = 0
            pattern = "[\.,;:'`?!，。？；：！]"
            long_data_list = []
            short_data_list = []
            for json_line in in_json_file.readlines():
                try:
                    json_object = json.loads(json_line)
                    keshi = json_object["keshi"]
                    if keshi not in keshi_list:
                        continue
                    #去除互为同义词的科室
                    if keshi in ['妇科综合', '妇科炎症']:
                        keshi = '妇科'
                    logger.debug("line:" + json_line)

                    if keshi not in keshi_ill_list:
                        continue

                    query = json_object["query"]
                    desc = json_object["desc"]

                    # 去重，去掉一些无关的标签
                    detail =  desc + query
                    logger.info("before preparser:%s" % detail)
                    detail = re.sub("病情描述（发病时间、主要症状、症状变化等）：", "", detail)  # 删除一些网站相关的文本
                    repeat_length, substr = find_longest_repeating_strings(detail)
                    if repeat_length != None and repeat_length >= 8:
                        detail = detail.replace(substr, "", 1)
                    logger.info("after preparser:%s" % detail)


                    #设置正则 判断分词结果是否包含中文
                    # zh_pattern = re.compile(u'[\u4e00-\u9fa5]+')
                    # 处理标题
                    #标题如果在 后面的detail中出现了，那么这个标题就不应该被写入文件，因为已经重复了
                    left_words_title = []
                    if str(query) not in detail.replace("\n", ""):
                        for sen in re.split(pattern, query):
                            sen = sen.strip()
                            terms = pyhanlp.HanLP.segment(sen)
                            words = [term.word for term in terms]
                            for word in words:
                                left_words_title.append(word)
                        sen_left = "".join(left_words_title)
                        if sen_left not in line_set:
                            line_set.add(sen_left)
                            if len(sen_left) > 0:
                                if len(sen_left) > 20:
                                    json_output_object = {"query": " ".join(left_words_title), "keshi": keshi}
                                    long_data_list.append(json_output_object)
                                else:
                                    json_output_object = {"query": " ".join(left_words_title), "keshi": keshi}
                                    short_data_list.append(json_output_object)
                                logger.info("keywords: " + sen_left)
                        pass


                    #处理标题+介绍
                    left_words = []
                    for sen in re.split(pattern, detail):
                        sen = sen.strip()
                        terms = pyhanlp.HanLP.segment(sen)
                        words = [term.word for term in terms]
                        for word in words:
                            left_words.append(word)
                    if len(left_words) - len(left_words_title) >= 5:
                        sen_left = "".join(left_words)
                        if sen_left not in line_set:
                            line_set.add(sen_left)
                            if len(sen_left) > 0:
                                if len(sen_left) > 20:
                                    json_output_object = {"query": " ".join(left_words), "keshi": keshi}
                                    long_data_list.append(json_output_object)
                                else:
                                    json_output_object = {"query": " ".join(left_words), "keshi": keshi}
                                    short_data_list.append(json_output_object)

                            logger.info("keywords: " + sen_left)

                except Exception as e:
                    error_str = traceback.format_exc()
                    logger.error(error_str)

            #按照小科室保存文件




            calc_time = datetime.datetime.now() - start_time
            host = socket.gethostname()
    except Exception as e:
        error_str = traceback.format_exc()
        logger.error(error_str)
    return host, in_file_path, long_result_dict, short_result_dict, calc_time, long_data_list, short_data_list


def job_callback(job):
    print("callback function")
    if job.status == dispy.DispyJob.Finished:  # most usual case
        # 'pending_jobs' is shared between two threads, so access it with
        # 'jobs_cond' (see below)
        host, in_file_path, long_result_dict, short_result_dict, calc_time, long_data_list, short_data_list = job()
        print('%s executed zip file: %s, used time : %s'
              % (host, in_file_path,  calc_time))



def stopwordslist(filepath):
    stopwords = [line.strip() for line in open(filepath, 'r', encoding='utf-8').readlines()]
    return stopwords


if __name__ == '__main__':
    import dispy, random, os
    import json
    import  xywy_keshi_map

    # 定义计算节点
    nodes = ["serverx27", "serverx28", "serverx29", "serverx32", "serverx33", "serverx41", "serverx42", "serverx44",
             "serverx43"]
    # 启动计算集群，和服务器通信，通信密钥是'secret'
    # depends 为依赖函数
    cluster = dispy.JobCluster(compute, nodes=nodes,
                               secret='secret', depends=[find_longest_repeating_strings, find_common_string], callback=job_callback)

    # 载入停用词列表
    stopword_list = stopwordslist("stopwords.txt")
    keshi_set = set()
    with codecs.open(r"keshi_list", "r", encoding="gbk",
                     errors="ignore") as f:
        for line in f.readlines():
            results = line.strip().split(" ")
            for result in results:
                keshi_set.add(result.strip())

    with codecs.open(r"ill_list", "r", encoding="gbk",
                     errors="ignore") as f:
        for line in f.readlines():
            result = line.strip()
            keshi_set.add(result)

    jobs = []

    file_list_path = "/home/liuml/file_list.dat"
    in_file_root = "/home/liuml/workspace/dispydemo/json_results"
    long_results_dir = "/home/liuml/workspace/dispydemo/long_results"
    short_results_dir = "/home/liuml/workspace/dispydemo/short_results"
    error_results_dir = "/home/liuml/workspace/dispydemo/error_results"

    with open(file_list_path, "r") as file_list:
        for file_name in file_list.readlines():
            # 提交任务
            file_name = file_name.strip()
            job = cluster.submit(
                (stopword_list, keshi_set, os.path.join(in_file_root, file_name + ".json"),
                 os.path.join(long_results_dir, file_name + ".json"),
                 os.path.join(short_results_dir, file_name + ".json"),
                 os.path.join(error_results_dir, file_name + ".error")))

            jobs.append(job)
    # print(datetime.datetime.now())
    cluster.wait()  # 等待所有任务完成后才接着往下执行
    # print(datetime.datetime.now())
    total_count = 0
    final_long_result_dict = {}
    final_short_result_dict = {}
    keshi_ir_map = {'呼吸内科': '呼吸内科', '哮喘': '呼吸内科', '肺气肿': '呼吸内科', '流感': '呼吸内科', '消化内科': '消化内科', '胃肠疾病': '消化内科',
                    '肝胆疾病': '消化内科', '肝硬化': '消化内科', '胃炎': '消化内科', '肝腹水': '消化内科', '酒精肝': '消化内科', '肝损伤': '消化内科',
                    '胆囊疾病': '消化内科', '肾内科': '肾内科', '肾病综合征': '肾内科', '肾炎': '肾内科', '尿毒症': '肾内科', '肾衰竭': '肾内科',
                    '急慢性肾炎': '肾内科',
                    '肾积水': '肾内科', '肾盂性肾炎': '肾内科', '内分泌科': '内分泌科', '内分泌': '内分泌科', '甲状腺疾病': '内分泌科', '糖尿病': '内分泌科',
                    '甲亢': '内分泌科', '心血管内科': '心血管内科', '高血压': '心血管内科', '心脏病': '心血管内科', '神经内科': '神经内科', '癫痫病': '神经内科',
                    '头痛': '神经内科', '面瘫': '神经内科', '帕金森': '神经内科', '脑萎缩': '神经内科', '中风': '神经内科', '截瘫': '神经内科',
                    '重症肌无力': '神经内科',
                    '肌肉萎缩': '神经内科', '脑外伤后遗症': '神经内科', '血液内科': '血液内科', '白血病': '血液内科', '风湿免疫': '风湿免疫', '痛风': '风湿免疫',
                    '强直性脊柱炎': '风湿免疫', '红斑狼疮': '风湿免疫', '白塞氏病': '风湿免疫', '普外科': '普外科', '肛肠疾病': '普外科', '痔疮': '普外科',
                    '疝气': '普外科',
                    '心胸外科': '心胸外科', '泌尿外科': '泌尿外科', '心血管外科': '心血管外科', '静脉曲张': '心血管外科', '肝血管瘤': '心血管外科', '脉管炎': '心血管外科',
                    '乳腺外科': '乳腺外科', '骨科': '骨科', '颈腰椎病': '骨科', '颈椎病': '骨科', '腰椎间盘突出': '骨科', '股骨头坏死': '骨科', '股骨头': '骨科',
                    '腰椎病': '骨科', '骨髓炎': '骨科', '下颌角': '骨科', '神经外科': '神经外科', '三叉神经痛': '神经外科', '脑血管病': '神经外科',
                    '脊髓病变': '神经外科',
                    '脑积水': '神经外科', '脑发育不全': '神经外科', '妇产科': '妇产科', '妇产科其它': '妇产科', '更年期综合症': '妇产科', '妇科': '妇科',
                    '妇科综合': '妇科',
                    '妇科炎症': '妇科', '月经不调': '妇科', '宫颈糜烂': '妇科', '子宫肌瘤': '妇科', '宫颈疾病': '妇科', '宫外孕': '妇科', '卵巢囊肿': '妇科',
                    '痛经': '妇科', '外阴白斑': '妇科', '产后妇科': '产后妇科', '产科': '产科', '产科综合': '产科', '人流': '产科', '不孕不育': '不孕不育',
                    '生殖孕育': '不孕不育', '助孕技术': '不孕不育', '男性不育': '男性不育', '女性不孕': '女性不孕', '皮肤科': '皮肤科', '皮肤综合': '皮肤科',
                    '白癜风': '皮肤科', '尖锐湿疣': '皮肤科', '牛皮癣': '皮肤科', '痤疮': '皮肤科', '湿疹': '皮肤科', '皮肤过敏': '皮肤科', '荨麻疹': '皮肤科',
                    '灰指甲': '皮肤科', '腋臭': '皮肤科', '梅毒': '皮肤科', '带状疱疹': '皮肤科', '烧烫伤': '皮肤科', '祛斑祛痣': '皮肤科', '植发': '皮肤科',
                    '鱼鳞病': '皮肤科', '胎记': '皮肤科', '斑秃': '皮肤科', '生发': '皮肤科', '祛痘祛疤': '皮肤科', '胎记治疗': '皮肤科', '黄褐斑': '皮肤科',
                    '毛发移植': '皮肤科', '洗纹身': '皮肤科', '扁平苔藓': '皮肤科', '狐臭': '皮肤科', '性病科': '性病科', '性病综合': '性病科',
                    '生殖器疱疹': '性病科',
                    '性功能障碍': '性病科', '淋病': '性病科', '男科': '男科', '前列腺': '男科', '早泄': '男科', '阳痿': '男科', '五官科综合': '五官科综合',
                    '耳鼻喉科': '耳鼻喉科', '鼻炎': '耳鼻喉科', '咽炎': '耳鼻喉科', '耳鸣耳聋': '耳鼻喉科', '中耳炎': '耳鼻喉科', '眼科': '眼科', '近视': '眼科',
                    '白内障': '眼科', '眼科炎症': '眼科', '青光眼': '眼科', '斜视': '眼科', '弱视': '眼科', '口腔科': '口腔科', '口臭': '口腔科',
                    '口腔溃疡': '口腔科', '牙周炎': '口腔科', '牙龈炎': '口腔科', '龋齿': '口腔科', '种植牙': '口腔科', '牙列不齐': '口腔科', '唇腭裂': '口腔科',
                    '口腔黏膜': '口腔科', '口疮': '口腔科', '牙科': '牙科', '牙齿美白': '牙科', '美容冠': '牙科', '精神科': '精神科', '精神科综合': '精神科',
                    '精神分裂症': '精神科', '妄想症': '精神科', '心理科': '心理科', '心理科其它': '心理科', '心理咨询': '心理科', '网络成瘾': '心理科',
                    '失眠抑郁': '心理科',
                    '强迫症': '心理科', '多动症': '心理科', '自闭症': '心理科', '心理障碍': '心理科', '社交恐慌': '心理科', '性心理障碍': '心理科',
                    '肿瘤科': '肿瘤科',
                    '血管瘤': '肿瘤科', '传染科': '传染科', '传染科综合': '传染科', '肝病': '传染科', '乙肝': '传染科', '肝炎': '传染科', '艾滋病': '传染科',
                    '狂犬病': '传染科', '结核病': '传染科', '丙肝': '传染科', '大小三阳': '传染科', '病毒性肝炎': '传染科', '水痘': '传染科',
                    '流行性腮腺炎': '传染科',
                    '急慢性肝炎': '传染科', '整形美容': '整形美容', '耳部整形': '整形美容', '丰唇': '整形美容', '面部整形': '整形美容', '肉毒素': '整形美容',
                    '整形其他': '整形美容', '口唇整形': '整形美容', '隆鼻': '整形美容', '整形美容其它': '整形美容', '自体脂肪移植': '整形美容', '美白针': '整形美容',
                    '皮肤美容': '整形美容', '美白祛斑': '整形美容', '疤痕': '整形美容', '丰胸美体': '整形美容', '除皱': '整形美容', '注射美容': '整形美容',
                    '美白嫩肤': '整形美容', '鼻部整形': '整形美容', '胸部整形': '整形美容', '吸脂减肥': '整形美容', '割双眼皮': '整形美容', '瘦脸': '整形美容',
                    '隆胸': '整形美容', '私密整形': '整形美容', '开眼角': '整形美容', '脱毛': '整形美容', '生殖器整形': '整形美容', '玻尿酸': '整形美容',
                    '去眼袋': '整形美容', '隆下巴': '整形美容', '体检': '体检', '眩晕科': '眩晕科', '结石科': '结石科', '外科其它': '外科其它', '外科': '外科其它',
                    '内科其它': '内科其它', '内科': '内科其它', '寄生虫疾病': '内科其它', '儿科': '儿科', '小儿外科': '儿科', '小儿综合': '儿科', '中医儿科': '儿科',
                    '小儿眼科': '儿科', '小儿内科': '儿科', '小儿脑瘫': '儿科', '小儿肥胖症': '儿科', '小儿哮喘': '儿科', '小儿厌食症': '儿科', '脑瘫': '儿科',
                    '新生儿疾病': '儿科', '抽动症': '儿科', '手足口病': '儿科', '性早熟': '儿科', '唐氏综合症': '儿科', '唐氏综合征': '儿科'}
    for job in jobs:
        host, in_file_path, long_result_dict, short_result_dict, calc_time, long_data_list, short_data_list = job()
        print('%s executed zip file: %s, used time: %s'
              % (host, in_file_path, calc_time))
        if long_data_list :
            for item in long_data_list:
                keshi = item["keshi"]
                if keshi not in keshi_ir_map:
                    category = keshi
                else:
                    category = keshi_ir_map[keshi]
                if not os.path.exists("详细疾病/"+"long/"+category):
                    os.makedirs("详细疾病/"+"long/"+category)
                with open("详细疾病/"+"long/"+category+"/"+keshi+".json", "a", encoding="utf8") as f:
                    f.writelines(str(item) + "\n")
        if short_data_list :
            for item in short_data_list:
                keshi = item["keshi"]
                if keshi not in keshi_ir_map:
                    category = keshi
                else:
                    category = keshi_ir_map[keshi]
                if not os.path.exists("详细疾病/"+"short/"+category):
                    os.makedirs("详细疾病/"+"short/"+category)
                with open("详细疾病/"+"short/"+category+"/"+keshi+".json", "a", encoding="utf8") as f:
                    f.writelines(str(item) + "\n")
            pass




    # 显示集群计算状态
    cluster.stats()
