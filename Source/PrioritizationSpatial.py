# importing pyspatialite
from pyspatialite import dbapi2 as db
import time


# Buffer the projects two ways: 1/4mi and 10ft. We do this to save time later on our intersects.
# Note that with PostGIS, this buffering step is silly because the search radius function DWithin() automatically
# uses the spatial indexing.

def buffproj10(cur):
    cur.execute("""CREATE TABLE proj10 AS
                   SELECT intprojid, CastToMulti(ST_Buffer(p.GEOMETRY, 10)) AS GEOMETRY
                   FROM mtp_projects_fin p""")

    cur.execute("""SELECT RecoverGeometryColumn('proj10', 'GEOMETRY', 2285, 'MULTIPOLYGON', 'XY')""")

    cur.execute("""SELECT CreateSpatialIndex('proj10', 'GEOMETRY')""")

def buffproj1320(cur):
    cur.execute("""CREATE TABLE proj1320 AS
                   SELECT intprojid, CastToMulti(ST_Buffer(p.GEOMETRY, 1320)) AS GEOMETRY
                   FROM mtp_projects_fin p""")
    
    cur.execute("""SELECT RecoverGeometryColumn('proj1320', 'GEOMETRY', 2285, 'MULTIPOLYGON', 'XY')""")

    cur.execute("""SELECT CreateSpatialIndex('proj1320', 'GEOMETRY')""")

def buffproj5280(cur):
    cur.execute("""CREATE TABLE proj5280 AS
                   SELECT intprojid, CastToMulti(ST_Buffer(p.GEOMETRY, 5280)) AS GEOMETRY
                   FROM mtp_projects_fin p""")
    
    cur.execute("""SELECT RecoverGeometryColumn('proj5280', 'GEOMETRY', 2285, 'MULTIPOLYGON', 'XY')""")

    cur.execute("""SELECT CreateSpatialIndex('proj5280', 'GEOMETRY')""")

def notProj(cur, projectlist):
    # identify projects NOT satisfying the condition
    res = cur.execute("""SELECT DISTINCT intprojid FROM proj10""")
    allproj = []
    for x in res:
        allproj += x

    inverse = list(set(allproj) - set(projectlist))
    return(inverse)


def intersect(cur, projects, layer, nonunique=None):
    if nonunique == None:
        sql = "SELECT DISTINCT p.intprojid "
    else:
        sql = "SELECT p.intprojid "
    sql += "FROM %s as p, %s " % (projects, layer)
    sql += "WHERE ST_Intersects(p.GEOMETRY, %s.GEOMETRY) AND %s.ROWID IN (SELECT ROWID FROM SpatialIndex WHERE f_table_name='%s' AND search_frame=p.GEOMETRY)" % (layer, layer, layer)

    res = cur.execute(sql)
    results = []
    for x in res:
        results += x
    return(results)

def multiOrSect(cur, projects, *layers):
    results = []
    for layer in layers:
        results += intersect(cur, projects, layer)
    return(results)

def multiAndSect(cur, projects, *layers):
    results = intersect(cur, projects, layers[0])
    for layer in layers[1:]:
        results = set.intersection(*map(set,intersect(cur, projects, layer)))
    return results


def processSpatial():
    # DB Connection
    conn = db.connect('C:/Temp/Prioritization/Prior.sqlite')


    # creating a Cursor
    cur = conn.cursor()

    # Run both buffering functions if they don't already exist
    if (u'proj10',) in cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='proj10'"):
        pass
    else:
        buffproj10(cur)

    if (u'proj1320',) in cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='proj1320'"):
        pass
    else:
        buffproj1320(cur)

    if (u'proj5280',) in cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='proj5280'"):
        pass
    else:
        buffproj5280(cur)

    # Stuff a dictionary with our results. The key is the question ID.
    results = {}

    # 98 - Air quality: is the project on a freight route?
    print("Processing 98")
    results['98'] = multiOrSect(cur, "proj10", "truck_bottlenecks_20120620", "t1t2")

    # 111 - Air quality: is the project within 1/4 mi buffer of schools?
    print("Processing 111")
    results['111'] = multiOrSect(cur, "proj1320", "school_location2")

    # 69 - Is the project on an identified truck bottleneck?
    print("Processing 69")
    results['69'] = multiOrSect(cur, "proj10", "truck_bottlenecks_20120620")

    # 72 - Is the project in an MIC? Does it connect two MICs or a MIC and an RGC?
    # NOTE: The first condition supercedes the remainder. Suspect this isn't what is wanted
    print("Processing 72")
    results['72'] = multiOrSect(cur, "proj10", "micto_urbcen_micnet3")

    # 73 - Is the project in an MIC?
    # NOTE: This and 72 are dupes
    print("Processing 73")
    results['73'] = multiOrSect(cur, "proj10", "micen")

    # 74 - Is the project within a TAZ identified as a freight generator
    print("Processing 74")
    results['74'] = multiOrSect(cur, "proj10", "freight_gen_taz2010")

    # 114 - Is the project on a T1/T2 route
    print("Processing 114")
    results['114'] = multiOrSect(cur, "proj10", "t1t2")


    # 66 - Within identified areas (18 jobs/acre density AND zoning)
    print("Processing 66")
    results['66'] = multiAndSect(cur, "proj10", "flu_jobs_32_lyr")

    # 67 - Within identified areas (18 jobs/acre density)
    print("Processing 67")
    results['67'] = multiOrSect(cur, "proj10", "all_jobs_18_lyr")

    # 68 - Within identified areas (15 jobs/acre density; cluster employment)
    print("Processing 68")
    results['68'] = multiOrSect(cur, "proj10", "cluster_15_lyr")

    # 106 - Within identified areas (15 jobs/acre density; family-wage)
    print("Processing 106")
    results['106'] = multiOrSect(cur, "proj10", "fmlywg_15_lyr")

    # 107 - Within some reasonable distance (1/4mi?) from identified economic foundation points
    print("Processing 107")
    results['107'] = multiOrSect(cur, "proj1320", "economic_foundations")


    # 116 - On the regional bicycle network
    print("Processing 116")
    results['116'] = multiOrSect(cur, "proj1320", "regional_bicycle_network")

    # 120 - Within 1/4mi of MTS transit stops
    print("Processing 120")
    results['120'] = multiOrSect(cur, "proj1320", "regional_transit")

    # 101 - Project is in a critical area
    print("Processing 101")
    results['101'] = notProj(cur, multiOrSect(cur, "proj10", "caoall", "priority_hab"))

    # 122 - Project is within identified resource lands
    results['122'] = multiOrSect(cur, "proj10", "reszone08_region")

    # 89 - On a facility with fatality, injury, or property damage incidents
    print("Processing 89")
    results['89'] = multiOrSect(cur, "proj10", "all_collisions")

    # 141 - On security recovery annex facility
    print("Processing 141")
    results['141'] = multiOrSect(cur, "proj10", "security")

    # 93 - In special needs area (NOTE: need guidance)
    print("Processing 93")
    results['93'] = multiOrSect(cur, "proj10", "all_four")

    # 150 - Connects area of low and very low to high or very high opp index
    print("Processing 150")
    results['150'] = multiOrSect(cur, "proj10", "low_to_highOP_net")
   
    # 151 - Connects to an area of low or very low
    print("Processing 151")
    results['151'] = multiOrSect(cur, "proj10", "low_verylow")
    
    # 152 - Connects to an area of high or very high
    print("Processing 152")
    results['152'] = multiOrSect(cur, "proj10", "high_veryhigh_opp")
   
    # 59 - Within an RGC
    print("Processing 59")
    results['59'] = multiOrSect(cur, "proj10", "urbcen")

    # 60 - Connect an RGC to RGC or MIC
    print("Processing 60")
    results['61'] = multiOrSect(cur, "proj10", "regcenToRegCenMICs_Net2")

    # 61 - Connect to RGC (1 mi)
    print("Processing 61")
    results['61'] = multiOrSect(cur, "proj5280", "urbcen")

    # 62 - In an area with housing density > 15
    print("Processing 62")
    results['62'] = multiOrSect(cur, "proj10", "housing_density_15_plus")

    # 63 - In an area with housing density > 8
    print("Processing 63")
    results['63'] = multiOrSect(cur, "proj10", "housing_density_8_to_15")


    # 75 - On an identified facility (bottleneck, chokepoint, congestion, etc.
    print("Processing 75")
    results['75'] = multiOrSect(cur, "proj10", "chokepoints_and_bottlenecks", "congested_transit_corridors", "its_key_arterial_corridors")

    return(results)

def uniqueProjectID():
    # DB Connection
    conn = db.connect('C:/Temp/Prioritization/Prior.sqlite')
    # creating a Cursor
    cur = conn.cursor()
    res = cur.execute("""SELECT DISTINCT intprojid FROM mtp_projects_fin""")
    allproj = []
    for x in res:
       allproj += x
    conn.close()
    return allproj


def main():
    #results = processSpatial()
    pass

if __name__ == "__main__":
     main()
