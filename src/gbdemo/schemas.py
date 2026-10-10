"""Column names for headerless source files, taken from the publishers' specifications.

NHSPD: "NHSPD User Guide Aug 2024" (ONS), record layout, 49 fields.
ODS epraccur: ODS file specification for GP practices, 27 fields (some unused, kept as nullN).
Bronze keeps every field as a string; silver applies types.
"""

NHSPD_FIELDS = [
    "pcd2",        # 1  postcode, fixed 8 characters (e.g. "AB1  0AA")
    "pcds",        # 2  postcode, single space (e.g. "AB1 0AA")
    "dointr",      # 3  date of introduction, YYYYMM
    "doterm",      # 4  date of termination, YYYYMM or null (null = live)
    "oseast100m",  # 5  easting, 100 m resolution
    "osnrth100m",  # 6  northing, 100 m resolution
    "oscty",       # 7  county
    "odslaua",     # 8  local authority organisation (ODS)
    "oslaua",      # 9  local authority district / unitary authority (GSS)
    "osward",      # 10 electoral ward / division
    "usertype",    # 11 0 = small user, 1 = large user
    "osgrdind",    # 12 grid reference quality: 1 = within building ... 9 = no grid reference
    "ctry",        # 13 country
    "oshlthau",    # 14 former SHA / LHB / health board
    "rgn",         # 15 region (former GOR)
    "oldha",       # 16 pre-2002 health authority
    "nhser",       # 17 NHS England region
    "sicbl",       # 18 sub ICB location / LHB / CHP
    "psed",        # 19 content removed
    "cened",       # 20 content removed
    "edind",       # 21 content removed
    "ward98",      # 22 content removed
    "oa01",        # 23 2001 Census output area
    "nhsrlo",      # 24 former NHS region, local office
    "hro",         # 25 former pan SHA
    "lsoa01",      # 26 2001 Census LSOA
    "ur01ind",     # 27 content removed
    "msoa01",      # 28 content removed
    "cannet",      # 29 former cancer network
    "scn",         # 30 strategic clinical network
    "oshaprev",    # 31 'first wave' SHA
    "oldpct",      # 32 'first wave' PCT
    "oldhro",      # 33 'old' IT cluster
    "pcon",        # 34 Westminster parliamentary constituency
    "canreg",      # 35 cancer registry
    "pct",         # 36 'second wave' PCT / care trust
    "oseast1m",    # 37 easting, 1 m resolution (BNG)
    "osnrth1m",    # 38 northing, 1 m resolution (BNG)
    "oa11",        # 39 2011 Census output area
    "lsoa11",      # 40 2011 Census LSOA
    "msoa11",      # 41 2011 Census MSOA
    "calncv",      # 42 cancer alliance
    "icb",         # 43 integrated care board, ODS code (e.g. QJK), not the GSS E54 code
    "smhpc_aed",   # 44 specialised mental health provider collaborative (eating disorders)
    "smhpc_as",    # 45 SMHPC (adult low and medium secure)
    "smhpc_ct4",   # 46 SMHPC (children and young people tier 4)
    "oa21",        # 47 2021 Census output area
    "lsoa21",      # 48 2021 Census LSOA
    "msoa21",      # 49 2021 Census MSOA
]

EPRACCUR_FIELDS = [
    "organisation_code",           # 1  ODS practice code, e.g. A81001
    "name",                        # 2
    "national_grouping",           # 3  NHS England region (Y56-Y63)
    "high_level_health_geography", # 4  ICB, ODS code
    "address_line_1",              # 5
    "address_line_2",              # 6
    "address_line_3",              # 7
    "address_line_4",              # 8
    "address_line_5",              # 9
    "postcode",                    # 10
    "open_date",                   # 11 YYYYMMDD
    "close_date",                  # 12 YYYYMMDD or empty
    "status_code",                 # 13 ACTIVE / INACTIVE / DORMANT
    "organisation_sub_type_code",  # 14
    "commissioner",                # 15 sub ICB location, ODS code
    "join_provider_date",          # 16
    "left_provider_date",          # 17
    "contact_telephone_number",    # 18
    "null19",                      # 19 unused
    "null20",                      # 20 unused
    "null21",                      # 21 unused
    "amended_record_indicator",    # 22
    "null23",                      # 23 unused
    "provider_purchaser",          # 24
    "null25",                      # 25 unused
    "prescribing_setting",         # 26 role code, e.g. RO76 = GP practice
    "null27",                      # 27 unused
]
