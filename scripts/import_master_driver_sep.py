#!/usr/bin/env python3
import json
import re
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
import xml.etree.ElementTree as ET

WORKBOOK = Path('/Users/admindevices/Downloads/Master Driver Sep.xlsx')
OUT_JSON = Path('/Users/admindevices/Downloads/Driver Management/data/db.json')

NS_MAIN = 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'
NS_REL = 'http://schemas.openxmlformats.org/officeDocument/2006/relationships'


def col_to_num(col: str) -> int:
    n = 0
    for ch in col:
        n = n * 26 + (ord(ch) - 64)
    return n


def excel_serial_to_iso(value: str) -> str:
    value = (value or '').strip()
    if value == '':
        return ''
    if re.fullmatch(r'\d+(?:\.0+)?', value):
        n = int(float(value))
        if n <= 0:
            return ''
        base = datetime(1899, 12, 30)
        return (base + timedelta(days=n)).strftime('%Y-%m-%d')
    return value


def normalize_text(v: str) -> str:
    return (v or '').replace('\r\n', '\n').replace('\r', '\n').strip()


def is_headerish(v: str, phrases) -> bool:
    lv = normalize_text(v).lower()
    if lv == '':
        return True
    return any(p in lv for p in phrases)


def load_workbook_rows(path: Path):
    with zipfile.ZipFile(path) as z:
        shared = []
        if 'xl/sharedStrings.xml' in z.namelist():
            ss = ET.fromstring(z.read('xl/sharedStrings.xml'))
            for si in ss.findall(f'{{{NS_MAIN}}}si'):
                shared.append(''.join(t.text or '' for t in si.findall(f'.//{{{NS_MAIN}}}t')))

        wb = ET.fromstring(z.read('xl/workbook.xml'))
        rels = ET.fromstring(z.read('xl/_rels/workbook.xml.rels'))
        relmap = {r.attrib['Id']: r.attrib['Target'] for r in rels}

        sheets = {}
        for s in wb.find(f'{{{NS_MAIN}}}sheets'):
            name = s.attrib['name']
            rid = s.attrib.get(f'{{{NS_REL}}}id')
            target = relmap.get(rid, '')
            if not target.startswith('worksheets/'):
                continue
            root = ET.fromstring(z.read('xl/' + target))
            rows = []
            for rr in root.findall(f'.//{{{NS_MAIN}}}sheetData/{{{NS_MAIN}}}row'):
                vals = {}
                for c in rr.findall(f'{{{NS_MAIN}}}c'):
                    m = re.match(r'([A-Z]+)(\d+)', c.attrib.get('r', ''))
                    if not m:
                        continue
                    col = col_to_num(m.group(1))
                    t = c.attrib.get('t')
                    inl = c.find(f'{{{NS_MAIN}}}is/{{{NS_MAIN}}}t')
                    v = c.find(f'{{{NS_MAIN}}}v')
                    if inl is not None:
                        val = inl.text or ''
                    elif v is None:
                        val = ''
                    else:
                        txt = v.text or ''
                        if t == 's':
                            try:
                                val = shared[int(txt)]
                            except Exception:
                                val = txt
                        else:
                            val = txt
                    vals[col] = val
                if vals:
                    mx = max(vals)
                    rows.append([vals.get(i, '') for i in range(1, mx + 1)])
            sheets[name] = rows
        return sheets


def find_header_row(rows, must_have):
    for i, row in enumerate(rows):
        joined = ' | '.join(normalize_text(x).lower() for x in row)
        if all(k in joined for k in must_have):
            return i, {normalize_text(v).lower(): idx for idx, v in enumerate(row)}
    raise RuntimeError(f'Header not found for keys: {must_have}')


def get(row, idx):
    if idx is None or idx >= len(row):
        return ''
    return normalize_text(row[idx])


def parse_drivers(rows):
    h, cols = find_header_row(rows, ['driver name', 'home base', 'availability'])
    out = []
    for row in rows[h + 1:]:
        name = get(row, cols.get('driver name:'))
        if is_headerish(name, ['driver name', 'past drivers', 'drivers that have been dismissed']):
            continue
        home = get(row, cols.get('home base:'))
        pos = get(row, cols.get('position / \ndivision')) or get(row, cols.get('position / division'))
        notes = get(row, cols.get('driver availability & constraints'))
        twic = get(row, cols.get('twic card')) or get(row, cols.get('twic card '))
        if not any([name, home, pos, notes, twic]):
            continue
        if name == '':
            continue
        out.append({
            'id': f'sep_{len(out)+1}',
            'name': name,
            'homeBase': home,
            'position': pos,
            'notes': notes,
            'twic': twic,
            'hiredCity': '',
            'currentCity': '',
            'preferredPartner': '',
            'routeRestrictions': '',
        })
    return out


def parse_leads(rows):
    h, cols = find_header_row(rows, ['driver name', 'date of position acceptance', 'date sent to phase 2'])
    out = []
    for row in rows[h + 1:]:
        name = get(row, cols.get('driver name'))
        if is_headerish(name, ['driver name']):
            continue
        rec = {
            'id': f'lead_{len(out)+1}',
            'name': name,
            'dateAccepted': excel_serial_to_iso(get(row, cols.get('date of position acceptance'))),
            'dateSentPhase2': excel_serial_to_iso(get(row, cols.get('date sent to phase 2'))),
            'position': get(row, cols.get('position')),
            'recruiter': get(row, cols.get('recruiter')),
            'notes': get(row, cols.get('notes')),
        }
        if not any(rec[k] for k in ['name', 'dateAccepted', 'dateSentPhase2', 'position', 'recruiter', 'notes']):
            continue
        if rec['name'] == '':
            continue
        out.append(rec)
    return out


def parse_otr(rows, prefix='otr'):
    h, cols = find_header_row(rows, ['driver name', 'ajg dt', 'gh dt', 'onboarding training'])
    out = []
    for row in rows[h + 1:]:
        name = get(row, cols.get('driver name'))
        if is_headerish(name, ['driver name']):
            continue
        rec = {
            'id': f'{prefix}_{len(out)+1}',
            'passed': excel_serial_to_iso(get(row, cols.get('passed'))),
            'name': name,
            'age': get(row, cols.get('age')),
            'position': get(row, cols.get('position')),
            'yoe': get(row, cols.get('years of experience')),
            'phone': get(row, cols.get('phone#')),
            'location': get(row, cols.get('location')),
            'ajgCH': get(row, cols.get('ajg ch')),
            'ghCH': get(row, cols.get('gh ch')),
            'i9': get(row, cols.get('i9')),
            'nhpw': get(row, cols.get('nhpw')),
            'ajgDT': get(row, cols.get('ajg dt')),
            'ghDT': get(row, cols.get('gh dt')),
            'onboarding': get(row, cols.get('onboarding training')),
            'insurance': get(row, cols.get('added to insurance')),
            'gtg': get(row, cols.get('gtg?')),
            'dispatched': get(row, cols.get('dispatched')),
            'notes': get(row, cols.get('notes')),
            'rtgDate': excel_serial_to_iso(get(row, cols.get('rtg date'))),
        }
        if not any(rec[k] for k in rec if k != 'id'):
            continue
        if rec['name'] == '':
            continue
        out.append(rec)
    return out


def parse_ag4_hires(rows):
    h, cols = find_header_row(rows, ['driver name', 'ag4 dt', 'dot medical', 'onboarding training'])
    out = []
    for row in rows[h + 1:]:
        name = get(row, cols.get('driver name'))
        if is_headerish(name, ['driver name']):
            continue
        rec = {
            'id': f'ag4_{len(out)+1}',
            'passed': excel_serial_to_iso(get(row, cols.get('passed'))),
            'name': name,
            'age': get(row, cols.get('age')),
            'position': get(row, cols.get('position')),
            'yoe': get(row, cols.get('yoe')),
            'phone': get(row, cols.get('phone#')),
            'location': get(row, cols.get('location')),
            'rtgDate': excel_serial_to_iso(get(row, cols.get('rtg date'))),
            'nhpw': get(row, cols.get('nhpw')),
            'i9': get(row, cols.get('i9 call completed')),
            'ag4DT': get(row, cols.get('ag4 dt')),
            'dotMedical': get(row, cols.get('dot medical test')),
            'onboarding': get(row, cols.get('onboarding training')),
            'insurance': get(row, cols.get('added to insurance')),
            'gtg': get(row, cols.get('gtg?')),
            'dispatched': get(row, cols.get('dispatched')),
            'notes': get(row, cols.get('notes')),
        }
        if not any(rec[k] for k in rec if k != 'id'):
            continue
        if rec['name'] == '':
            continue
        out.append(rec)
    return out


def parse_ag4_sep(rows):
    h, cols = find_header_row(rows, ['driver name', 'ag4 dt', 'onboarding training', 'return date'])
    out = []
    for row in rows[h + 1:]:
        name = get(row, cols.get('driver name'))
        if is_headerish(name, ['driver name']):
            continue
        rec = {
            'id': f'ag4sep_{len(out)+1}',
            'passed': excel_serial_to_iso(get(row, cols.get('passed'))),
            'name': name,
            'nhpw': get(row, cols.get('nhpw')),
            'ag4DT': get(row, cols.get('ag4 dt')),
            'onboarding': get(row, cols.get('onboarding training')),
            'insurance': get(row, cols.get('added to insurance')),
            'gtg': get(row, cols.get('gtg?')),
            'dispatched': get(row, cols.get('dispatched')),
            'notes': get(row, cols.get('notes')),
            'rtgDate': excel_serial_to_iso(get(row, cols.get('rtg date'))),
            'returnDate': excel_serial_to_iso(get(row, cols.get('return date'))),
            'location': get(row, cols.get('location')),
            'phone': get(row, cols.get('number')),
            'position': get(row, cols.get('position')),
        }
        if not any(rec[k] for k in rec if k != 'id'):
            continue
        if rec['name'] == '':
            continue
        out.append(rec)
    return out


def parse_historical(rows):
    h, cols = find_header_row(rows, ['last name', 'first name', 'termination date'])
    out = []
    for row in rows[h + 1:]:
        ln = get(row, cols.get('last name'))
        if is_headerish(ln, ['last name', 'past drivers', 'drivers that have been dismissed']):
            continue
        rec = {
            'id': f'hist_{len(out)+1}',
            'lastName': ln,
            'firstName': get(row, cols.get('first name')),
            'position': get(row, cols.get('position')),
            'terminationDate': excel_serial_to_iso(get(row, cols.get('termination date'))),
            'incentives': get(row, cols.get('incentives')) or get(row, cols.get('incentives ')),
        }
        if not any(rec[k] for k in rec if k != 'id'):
            continue
        if rec['lastName'] == '':
            continue
        out.append(rec)
    return out


def dedupe_otr(records):
    seen = set()
    out = []
    for r in records:
        key = (
            normalize_text(r.get('name', '')).lower(),
            normalize_text(r.get('phone', '')),
            normalize_text(r.get('passed', '')),
            normalize_text(r.get('rtgDate', '')),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    for i, r in enumerate(out, 1):
        r['id'] = f'otr_{i}'
    return out


def main():
    sheets = load_workbook_rows(WORKBOOK)

    drivers = parse_drivers(sheets['Driver Utilization (Driver Sep)'])
    leads = parse_leads(sheets['LEADS'])
    otr_main = parse_otr(sheets['OTR New Hires Status'], prefix='otrmain')
    otr_ajggh = parse_otr(sheets['AJGGH New Hires Status'], prefix='otrajggh')
    otr = dedupe_otr(otr_main + otr_ajggh)
    ag4_hires = parse_ag4_hires(sheets['AG4 New Hire Status'])
    ag4_sep = parse_ag4_sep(sheets['AG4 Driver Sep'])
    historical = parse_historical(sheets['Historical Drivers'])

    db = {
        'driversSep': drivers,
        'leads': leads,
        'otrHires': otr,
        'ag4Hires': ag4_hires,
        'ag4Sep': ag4_sep,
        'historical': historical,
    }

    OUT_JSON.write_text(json.dumps(db, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')

    print('Import complete:')
    for k, v in db.items():
        print(f'- {k}: {len(v)}')


if __name__ == '__main__':
    main()
