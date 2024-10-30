import re


def sub_str(text):
    return re.sub(r'\s*[\(\（\[【<《][^\)\）\]】>》]*[\)\）\]】>》]\s*', '', text)

def contains_chinese(text):
    """判断字符串是否包含中文字符"""
    pattern = re.compile(r'[\u4e00-\u9fa5]')
    return bool(pattern.search(text))

def extract_chinese(text):
    """提取字符串中的中文字符"""
    pattern = re.compile(r'[\u4e00-\u9fa5]+')
    return pattern.findall(text)[0]

def change_str(text):
    text = sub_str(text)
    if contains_chinese(text):
        text = extract_chinese(text)
    return text

