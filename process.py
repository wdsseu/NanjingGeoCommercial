import pandas as pd
import json
import os
import ast
import re

# 配置参数
DATA_DIR = "data"
YEARS = range(2022, 2025) 
POI_TYPES = [
    "商务住宅", "科教文化服务", "购物服务", "交通设施服务", "餐饮服务", 
    "公司企业", "生活服务", "体育休闲服务", "医疗保健服务", "政府机构及社会团体", 
    "住宿服务", "金融保险服务", "机动车服务", "风景名胜", "公共设施", 
    "地名地址信息", "汽车维修"
]

# 核心 ID 映射配置
ID_MAP = {
    "PLOT": "DIKUAI_ID",
    "SUBWAY_STATION": "DITIEZHANDIAN_ID",
    "SUBWAY_LINE": "DITIEXIAN_ID",
    "ROAD": "DAOLU_ID"
}

# 地块 ID 归一化
def to_global_id(text):
    if pd.isna(text): return None
    s = str(text).strip().replace('\n', '').replace('\r', '')
    if s.endswith('.0'): s = s[:-2]
    match = re.search(r'(\d+)$', s)
    if match:
        return f"DK_{match.group(1).zfill(4)}"
    return s

# 地铁站 ID 归一化
def to_station_id(text):
    if pd.isna(text): return None
    s = str(text).strip()
    if s.endswith('.0'): s = s[:-2]
    match = re.search(r'(\d+)$', s)
    if match:
        return f"ST_{match.group(1)}"
    return s

def clean_val(v):
    try:
        if pd.isna(v): return 0.0
        return float(v)
    except:
        return 0.0

def parse_polygon_path(geo_raw):
    if pd.isna(geo_raw) or str(geo_raw).strip() == "": return []
    try:
        data = ast.literal_eval(str(geo_raw).strip())
        if isinstance(data, list) and len(data) > 0:
            curr = data
            while isinstance(curr, list) and len(curr) > 0 and isinstance(curr[0], list) and isinstance(curr[0][0], list):
                curr = curr[0]
            return [[round(float(p[0]), 6), round(float(p[1]), 6)] for p in curr if len(p) >= 2]
    except:
        nums = re.findall(r"[-+]?\d*\.\d+|\d+", str(geo_raw))
        it = iter(nums)
        return [[round(float(lng), 6), round(float(lat), 6)] for lng, lat in zip(it, it)]
    return []

def find_column(df, possible_names):
    cols = df.columns.tolist()
    for p in possible_names:
        for c in cols:
            if p.lower() in str(c).strip().lower():
                return c
    return None

def force_read_table(file_base_name):
    for ext in ['.xlsx', '.csv']:
        file_path = os.path.join(DATA_DIR, f"{file_base_name}{ext}")
        if not os.path.exists(file_path): continue
        try:
            if ext == '.csv':
                for enc in ['utf-8-sig', 'gb18030', 'gbk', 'utf-8']:
                    try: return pd.read_csv(file_path, encoding=enc, low_memory=False)
                    except: continue
            else:
                return pd.read_excel(file_path)
        except: continue
    return None

def generate_json():
    print(f"开始生成JSON文件...")
    nodes_dict, all_road_geo, all_road_relations = {}, {}, {}
    subway_stations_geo, subway_lines_geo, plot_subway_relations = {}, {}, {}
    global_links_set = set() 
    station_id_to_name = {}

    for year in YEARS:
        y_str = str(year)
        print(f"正在处理: {y_str} 年数据...")

        # 加载数据
        df_geo = force_read_table(f"地块/{year}地块_几何信息")
        df_attr = force_read_table(f"地块/{year}地块属性信息")
        df_poi = force_read_table(f"地块/{year}地块_POI关联结果")
        df_adj = force_read_table(f"地块/{year}地块邻接关系")
        df_road_geo = force_read_table(f"道路/{year}道路_几何信息")
        df_road_rel = force_read_table(f"道路/{year}道路-地块")
        df_st_geo = force_read_table(f"地铁/{year}地铁站点_几何信息")
        df_li_geo = force_read_table(f"地铁/{year}地铁线路_几何信息")
        df_radius = force_read_table(f"地铁/{year}地铁站（半径500m）-地块")
        df_st_line_rel = force_read_table(f"地铁/{year}地铁站-关联-地铁线路")

        # 地块基础处理
        if df_geo is not None and df_attr is not None:
            p_id = ID_MAP["PLOT"]
            
            far_col = find_column(df_attr, ["容积率"])
            dens_col = find_column(df_attr, ["建筑密"])
            height_col = find_column(df_attr, ["MEAN_高", "高度"])
            area_col = find_column(df_attr, ["地块面", "面积"])
            path_col = find_column(df_geo, ["完整GeoJSON", "坐标", "geometry"])

            df_attr['gid'] = df_attr[p_id].apply(to_global_id)
            df_geo['gid'] = df_geo[p_id].apply(to_global_id)
            df_combined = pd.merge(df_attr, df_geo, on='gid', how='inner', suffixes=('', '_geo'))

            # POI 聚合
            poi_counts_map, poi_names_map = {}, {}
            if df_poi is not None and p_id in df_poi.columns:
                df_poi['gid'] = df_poi[p_id].apply(to_global_id)
                if 'type1' in df_poi.columns:
                    poi_counts_map = df_poi.groupby(['gid', 'type1']).size().unstack(fill_value=0).to_dict('index')
                
                def get_top_10_names(group):
                    names = group.dropna().unique().tolist()
                    if not names: return "暂无机构记录"
                    return "、".join(names[:10]) + (" 等..." if len(names) > 10 else "")
                
                if 'name' in df_poi.columns:
                    poi_names_map = df_poi.groupby('gid')['name'].apply(get_top_10_names).to_dict()

            for _, row in df_combined.iterrows():
                gid = row['gid']
                if gid not in nodes_dict:
                    nodes_dict[gid] = {
                        "id": gid,
                        "path": parse_polygon_path(row.get(path_col)) if path_col else [],
                        "lng": (clean_val(row.get('最小经度')) + clean_val(row.get('最大经度'))) / 2,
                        "lat": (clean_val(row.get('最小纬度')) + clean_val(row.get('最大纬度'))) / 2,
                        "static_props": {
                            "用地性质": str(row.get('用地性质', '未知')),
                            "DIKUAI_ID": str(row.get(p_id, gid)),
                            "地块全称": str(row.get(p_id, gid)),
                            "规划面积": clean_val(row.get(area_col))
                        },
                        "temporal_physic": {"far": {}, "density": {}, "height": {}, "poi_names": {}},
                        "time_series": {}
                    }
                
                nodes_dict[gid]["temporal_physic"]["far"][y_str] = clean_val(row.get(far_col))
                nodes_dict[gid]["temporal_physic"]["density"][y_str] = clean_val(row.get(dens_col))
                nodes_dict[gid]["temporal_physic"]["height"][y_str] = clean_val(row.get(height_col))
                nodes_dict[gid]["temporal_physic"]["poi_names"][y_str] = poi_names_map.get(gid, "暂无机构记录")
                
                if gid in poi_counts_map:
                    counts = [poi_counts_map[gid].get(t, 0) for t in POI_TYPES]
                    total = sum(counts)
                    nodes_dict[gid]["time_series"][y_str] = [round(c/total, 3) if total > 0 else 0 for c in counts]
                else:
                    nodes_dict[gid]["time_series"][y_str] = [0] * len(POI_TYPES)

        # 邻接关系
        if df_adj is not None:
            for _, row in df_adj.iterrows():
                raw_u = row.get('From') if 'From' in df_adj.columns else row.iloc[0]
                raw_v = row.get('To') if 'To' in df_adj.columns else row.iloc[1]
                
                u, v = to_global_id(raw_u), to_global_id(raw_v)
                
                if u and v and u in nodes_dict and v in nodes_dict:
                    global_links_set.add(tuple(sorted((u, v))))

        # 道路
        if df_road_geo is not None:
            r_id_col = ID_MAP["ROAD"]
            for _, row in df_road_geo.iterrows():
                rid = str(row.get(r_id_col, '')).strip()
                if rid and rid not in all_road_geo:
                    try:
                        coords = ast.literal_eval(str(row.get('经纬度坐标点列表', '[]')))
                        all_road_geo[rid] = {"coords": [[float(p[1]), float(p[0])] for p in coords], "name": str(row.get('道路名称', '未知'))}
                    except: continue

        if df_road_rel is not None:
            p_id, r_id = ID_MAP["PLOT"], ID_MAP["ROAD"]
            curr_road_map = {}
            for _, row in df_road_rel.iterrows():
                sid, rid = to_global_id(row.get(p_id)), str(row.get(r_id, '')).strip()
                if sid in nodes_dict:
                    curr_road_map.setdefault(sid, []).append(rid)
            all_road_relations[y_str] = curr_road_map

        # 地铁
        if df_st_geo is not None:
            st_id_col = ID_MAP["SUBWAY_STATION"]
            for _, row in df_st_geo.iterrows():
                norm_sid = to_station_id(row.get(st_id_col))
                s_name = str(row.get('地铁站点名称', '未知')).strip()
                if norm_sid: station_id_to_name[norm_sid] = s_name 
                fid = f"STATION_{s_name}"
                if fid not in subway_stations_geo:
                    try:
                        c_list = ast.literal_eval(str(row.get('经纬度坐标列表', '[]')))
                        coords = [float(c_list[0][0]), float(c_list[0][1])] if c_list else [0,0]
                        subway_stations_geo[fid] = {
                            "name": s_name, 
                            "coords": coords, 
                            "stationId": fid,
                            "lines": []
                        }
                    except: pass
        if df_st_line_rel is not None:
            for _, row in df_st_line_rel.iterrows():
                raw_sid = row.get('DITIEZHANDIAN_ID')
                line_name = str(row.get('DITIEXIAN_ID', '')).strip()
                
                norm_sid = to_station_id(raw_sid)
                s_name = station_id_to_name.get(norm_sid)
                
                if s_name:
                    fid = f"STATION_{s_name}"
                    if fid in subway_stations_geo:
                        if line_name and line_name not in subway_stations_geo[fid]["lines"]:
                            subway_stations_geo[fid]["lines"].append(line_name)

        if df_li_geo is not None:
            li_id_col = ID_MAP["SUBWAY_LINE"]
            for _, row in df_li_geo.iterrows():
                lid = str(row.get(li_id_col, '')).strip()
                if lid and lid not in subway_lines_geo:
                    try:
                        l_coords = ast.literal_eval(str(row.get('经纬度坐标列表', '[]')))
                        subway_lines_geo[lid] = {"name": lid, "coords": [[float(p[0]), float(p[1])] for p in l_coords]}
                    except: continue

        if df_radius is not None:
            st_id_col, p_id = ID_MAP["SUBWAY_STATION"], ID_MAP["PLOT"]
            y_rad_map = {}
            for _, row in df_radius.iterrows():
                norm_st_id, gid = to_station_id(row.get(st_id_col)), to_global_id(row.get(p_id))
                s_name = station_id_to_name.get(norm_st_id)
                if s_name and (gid in nodes_dict):
                    fid = f"STATION_{s_name}"
                    y_rad_map.setdefault(fid, [])
                    if gid not in y_rad_map[fid]: y_rad_map[fid].append(gid)
            plot_subway_relations[y_str] = y_rad_map

    final_output = {
        "labels": POI_TYPES,
        "nodes": list(nodes_dict.values()),
        "links": [{"source": e[0], "target": e[1]} for e in global_links_set if e[0] in nodes_dict and e[1] in nodes_dict],
        "roads": {"geo": all_road_geo, "relations": all_road_relations},
        "subway": {"stations": subway_stations_geo, "lines": subway_lines_geo, "plot_links": plot_subway_relations}
    }
    
    with open('data.json', 'w', encoding='utf-8') as f:
        json.dump(final_output, f, ensure_ascii=False, indent=2)
    print(f"处理完成！data.json 已生成")

if __name__ == "__main__":
    generate_json()