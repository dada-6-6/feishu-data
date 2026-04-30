# -*- coding: utf-8 -*-
"""
飞书多维表 → data.json 同步脚本
每天自动从飞书多维表拉取任务数据，生成 HTML 页面所需的 data.json
"""

import requests
import json
import os
import sys
from datetime import datetime, timezone, timedelta

# 飞书 DateTime 字段返回 UTC 时间戳，统一转换为东八区（UTC+8）显示
TZ_CN = timezone(timedelta(hours=8))

# ============ 配置 ============
# 优先从环境变量读取（GitHub Actions 使用），否则用默认值（本地运行）
APP_ID = os.environ.get("FEISHU_APP_ID", "cli_a9311133487a9cd2")
APP_SECRET = os.environ.get("FEISHU_APP_SECRET", "3oaVGDWtehd4wDV7i0dM9d7AQAX3xepB")
APP_TOKEN = "JWV3bVG68aZSN0sr8VecH48Tnqh"
TABLE_ID = "tblTS9KSxjQeMhGG"
# 本地路径或 GitHub Actions 的工作目录
OUTPUT_PATH = os.environ.get("OUTPUT_PATH", r"D:\哒哒测试\兼职任务查询页案例\data.json")

# 飞书字段名（中文名） → data.json 字段名
FIELD_MAP = {
    "ID": "id",
    "任务ID": "taskId",
    "主任务ID": "mainTaskId",
    "子任务ID序号": "subTaskNo",
    "任务名称": "taskName",
    "制作类型": "taskType",
    "PPT兼职": "partTimer",
    "创建人": "creator",
    "质检人": "reviewer",
    "学科": "subject",
    "年级": "grade",
    "题量": "quantity",
    "修改题量": "editQuantity",
    "制作状态": "productionStatus",
    "分配状态": "allocationStatus",
    "兼职需交付日期": "dueDate",
    "质检审核完成日期": "qcReviewCompletedDate",
    "视频要求交付": "videoDueDate",
    "创建时间": "createdAt",
    "任务分配时间": "assignedAt",
    "商家名称-主账号": "merchantMain",
    "商家名称-子账号": "merchantSub",
    "需求ID": "requirementId",
    "单价": "unitPrice",
    "本月质检单价": "monthQcPrice",
    "学年质检单价": "yearQcPrice",
    "扣题": "deductedQuestions",
    "抽检备注": "inspectionNote",
    "提成": "commission",
}

# 需要从数组 [{text: "xxx"}] 中提取文本的字段
ARRAY_TEXT_FIELDS = {"学段", "月份-创建日期", "月份-审核通过", "春通是否流入", "直接流转", "自定义ID"}

# DateTime 类型字段（飞书返回毫秒时间戳 → "2026-04-21"）
DATETIME_FIELDS = {"创建时间", "兼职需交付日期", "视频要求交付", "质检审核完成日期"}

# Number 类型字段
NUMBER_FIELDS = {"题量", "修改题量", "单价", "主任务ID", "子任务ID序号", "ID", "扣题"}

# ModifiedTime 类型字段（返回毫秒时间戳）
MODIFIED_TIME_FIELDS = {"任务分配时间"}

# 只拉取以下制作状态的记录（空值也跳过）
ALLOWED_STATUSES = {"排版中", "已完成", "排版内部驳回待修改"}


def get_tenant_token():
    """获取 tenant_access_token"""
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    resp = requests.post(url, json={"app_id": APP_ID, "app_secret": APP_SECRET}, timeout=30)
    data = resp.json()
    if data.get("code") != 0:
        print(f"[ERROR] 获取 token 失败: {data}")
        sys.exit(1)
    return data["tenant_access_token"]


def convert_value(field_name, value):
    """将飞书字段值转换为 data.json 所需格式"""
    if value is None or value == "":
        return ""
    
    # 数组文本字段 [{text: "xxx"}] → "xxx"
    if field_name in ARRAY_TEXT_FIELDS:
        if isinstance(value, list) and value:
            if isinstance(value[0], dict):
                return value[0].get("text", "")
            return str(value[0])
        return ""
    
    # DateTime 字段（毫秒时间戳 → "2026-04-21"，强制 UTC+8）
    if field_name in DATETIME_FIELDS:
        try:
            ts = int(value)
            if ts > 0:
                return datetime.fromtimestamp(ts / 1000, tz=TZ_CN).strftime("%Y-%m-%d %H:%M")
            return ""
        except (ValueError, TypeError, OSError):
            return ""
    
    # ModifiedTime 字段（毫秒时间戳 → "2026-04-21"，强制 UTC+8）
    if field_name in MODIFIED_TIME_FIELDS:
        try:
            ts = int(value)
            if ts > 0:
                return datetime.fromtimestamp(ts / 1000, tz=TZ_CN).strftime("%Y-%m-%d %H:%M")
            return ""
        except (ValueError, TypeError, OSError):
            return ""
    
    # Number 字段
    if field_name in NUMBER_FIELDS:
        try:
            num = float(value)
            if num == int(num):
                return int(num)
            return num
        except (ValueError, TypeError):
            return ""
    
    return value


def fetch_all_records(token):
    """分页拉取所有记录"""
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records"
    headers = {"Authorization": f"Bearer {token}"}
    
    all_records = []
    page_token = None
    page = 0
    
    while True:
        params = {"page_size": 500, "automatic_fields": False}
        if page_token:
            params["page_token"] = page_token
        
        resp = requests.get(url, headers=headers, params=params, timeout=30)
        data = resp.json()
        
        if data.get("code") != 0:
            print(f"[ERROR] 拉取记录失败 (page {page}): {data}")
            sys.exit(1)
        
        items = data["data"].get("items", [])
        all_records.extend(items)
        page += 1
        
        has_more = data["data"].get("has_more", False)
        if not has_more:
            break
        page_token = data["data"].get("page_token")
        print(f"  已拉取 {len(all_records)} 条...")
    
    return all_records


def record_to_json(record):
    """将飞书 record 转换为 data.json 格式"""
    fields = record.get("fields", {})
    result = {}
    
    for feishu_name, json_key in FIELD_MAP.items():
        raw_value = fields.get(feishu_name, "")
        result[json_key] = convert_value(feishu_name, raw_value)
    
    return result


def main():
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 开始同步飞书多维表数据...")
    
    # 1. 获取 token
    print("  获取 token...")
    token = get_tenant_token()
    print("  Token OK")
    
    # 2. 拉取记录并筛选
    print("  拉取多维表记录...")
    records = fetch_all_records(token)
    print(f"  共拉取 {len(records)} 条原始记录")
    
    # 3. 按制作状态筛选 + PPT兼职为空跳过 + 转换格式
    print(f"  筛选状态: {', '.join(ALLOWED_STATUSES)}")
    data = []
    skipped_status = 0
    skipped_no_parttimer = 0
    for rec in records:
        # 跳过不符合状态的记录
        status = rec.get("fields", {}).get("制作状态", "")
        if status not in ALLOWED_STATUSES:
            skipped_status += 1
            continue
        # 跳过 PPT兼职为空的记录
        part_timer = rec.get("fields", {}).get("PPT兼职", "")
        if not part_timer or (isinstance(part_timer, str) and not part_timer.strip()):
            skipped_no_parttimer += 1
            continue
        item = record_to_json(rec)
        data.append(item)
    
    print(f"  筛选后剩余 {len(data)} 条（跳过状态不符 {skipped_status} 条，PPT兼职为空 {skipped_no_parttimer} 条）")
    
    # 4. 写入文件
    output_dir = os.path.dirname(OUTPUT_PATH)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    
    print(f"  已写入 {OUTPUT_PATH} ({len(data)} 条)")
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 同步完成！")


if __name__ == "__main__":
    main()
