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
    import codecs, json, traceback, socket, datetime, re, pyhanlp, logging
    # 定义log记录
    logger = logging.getLogger(__name__)
    logger.setLevel(level=logging.DEBUG)
    handler = logging.FileHandler(log_file_path)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    logger.addHandler(handler)
    try:
        long_result_dict = {}
        short_result_dict = {}

        start_time = datetime.datetime.now()
        line_set = set()
        with codecs.open(in_file_path, encoding="utf8") as in_json_file:
            count = 0
            pattern = "[\.,;:'`?!，。？；：！]"
            short_file = codecs.open(short_file_path, "w", encoding="utf8")
            long_file = codecs.open(long_file_path, "w", encoding="utf8")
            for json_line in in_json_file.readlines():
                try:
                    json_object = json.loads(json_line)
                    keshi = json_object["keshi"]
                    logger.debug("line:" + json_line)

                    if keshi not in keshi_ill_list:
                        continue

                    query = json_object["query"]
                    desc = json_object["desc"]

                    # 去重，去掉一些无关的标签
                    detail = query + desc
                    logger.info("before preparser:%s" % detail)
                    detail = re.sub("病情描述（发病时间、主要症状、症状变化等）：", "", detail)  # 删除一些网站相关的文本
                    repeat_length, substr = find_longest_repeating_strings(detail)
                    if repeat_length != None and repeat_length >= 8:
                        detail = detail.replace(substr, "", 1)
                    logger.info("after preparser:%s" % detail)


                    #设置正则 判断分词结果是否包含中文
                    zh_pattern = re.compile(u'[\u4e00-\u9fa5]+')
                    # 处理标题
                    left_words_title = []
                    for sen in re.split(pattern, query):
                        sen = sen.strip()
                        terms = pyhanlp.HanLP.segment(sen)
                        words = [term.word for term in terms]
                        for word in words:
                            if word in stopword_list:
                                continue
                            elif word.isdigit():
                                continue
                            elif not zh_pattern.search(word):
                                continue
                            else:
                                left_words_title.append(word)
                    sen_left = "".join(left_words_title)
                    if sen_left not in line_set:
                        line_set.add(sen_left)
                        if len(sen_left) > 0:
                            if len(sen_left) > 20:
                                if keshi in long_result_dict:
                                    long_result_dict[keshi] += 1
                                else:
                                    long_result_dict[keshi] = 1
                                json_output_object = {"query": " ".join(left_words_title), "keshi": keshi}
                                long_file.writelines(json.dumps(json_output_object, ensure_ascii=False) + "\n")
                            else:
                                if keshi in short_result_dict:
                                    short_result_dict[keshi] += 1
                                else:
                                    short_result_dict[keshi] = 1
                                json_output_object = {"query": " ".join(left_words_title), "keshi": keshi}
                                short_file.writelines(json.dumps(json_output_object, ensure_ascii=False) + "\n")
                            logger.info("keywords: " + sen_left)

                    #处理标题+介绍
                    left_words = []
                    for sen in re.split(pattern, detail):
                        sen = sen.strip()
                        terms = pyhanlp.HanLP.segment(sen)
                        words = [term.word for term in terms]
                        for word in words:
                            if word in stopword_list:
                                continue
                            elif word.isdigit():
                                continue
                            elif not zh_pattern.search(word):
                                continue
                            else:
                                left_words.append(word)
                    if len(left_words) - len(left_words_title) >= 5:
                        sen_left = "".join(left_words)
                        if sen_left not in line_set:
                            line_set.add(sen_left)
                            if len(sen_left) > 0:
                                if len(sen_left) > 20:
                                    if keshi in long_result_dict:
                                        long_result_dict[keshi] += 1
                                    else:
                                        long_result_dict[keshi] = 1
                                    json_output_object = {"query": " ".join(left_words), "keshi": keshi}
                                    long_file.writelines(json.dumps(json_output_object, ensure_ascii=False) + "\n")
                                else:
                                    if keshi in short_result_dict:
                                        short_result_dict[keshi] += 1
                                    else:
                                        short_result_dict[keshi] = 1
                                    json_output_object = {"query": " ".join(left_words), "keshi": keshi}
                                    short_file.writelines(json.dumps(json_output_object, ensure_ascii=False) + "\n")
                            logger.info("keywords: " + sen_left)

                except Exception as e:
                    error_str = traceback.format_exc()
                    logger.error(error_str)

            long_file.flush()
            long_file.close()
            short_file.flush()
            short_file.close()
            calc_time = datetime.datetime.now() - start_time
            host = socket.gethostname()
    except Exception as e:
        error_str = traceback.format_exc()
        logger.error(error_str)
    return host, in_file_path, long_result_dict, short_result_dict, calc_time


def job_callback(job):
    print("callback function")
    if job.status == dispy.DispyJob.Finished:  # most usual case
        # 'pending_jobs' is shared between two threads, so access it with
        # 'jobs_cond' (see below)
        host, in_file_path, long_result_dict, short_result_dict, calc_time = job()
        print('%s executed zip file: %s, used time: %s'
              % (host, in_file_path,  calc_time))


def stopwordslist(filepath):
    stopwords = [line.strip() for line in open(filepath, 'r', encoding='utf-8').readlines()]
    return stopwords


if __name__ == '__main__':
    import dispy, random, os

    # 定义计算节点
    nodes = ["serverx27", "serverx28", "serverx29", "serverx32", "serverx33", "serverx41", "serverx42", "serverx44",
             "serverx43"]
    # 启动计算集群，和服务器通信，通信密钥是'secret'
    # depends 为依赖函数
    cluster = dispy.JobCluster(compute, nodes=nodes,
                               secret='secret', depends=[find_longest_repeating_strings, find_common_string])

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
    for job in jobs:
        host, in_file_path, long_result_dict, short_result_dict, calc_time = job()
        print('%s executed zip file: %s, used time: %s'
              % (host, in_file_path, calc_time))

        # 长句训练集合reduce
        for keshi in long_result_dict.keys():
            if keshi in final_long_result_dict:
                final_long_result_dict[keshi] += long_result_dict[keshi]
            else:
                final_long_result_dict[keshi] = long_result_dict[keshi]

        # 短句训练集合reduce
        for keshi in short_result_dict.keys():
            if keshi in final_short_result_dict:
                final_short_result_dict[keshi] += short_result_dict[keshi]
            else:
                final_short_result_dict[keshi] = short_result_dict[keshi]

    final_long_result_dict = sorted(list(zip(final_long_result_dict.keys(), final_long_result_dict.values())),
                                    key=lambda x: x[1],
                                    reverse=True)
    final_short_result_dict = sorted(list(zip(final_short_result_dict.keys(), final_short_result_dict.values())),
                                     key=lambda x: x[1],
                                     reverse=True)

    for item in final_long_result_dict:
        print(item)

    for item in final_short_result_dict:
        print(item)
    # 显示集群计算状态
    cluster.stats()
