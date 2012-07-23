import psycopg2 as db
import time



def intersect(cur, searchradius, layer, nonunique=None):
    if nonunique == None:
        sql = "SELECT DISTINCT p.intprojid "
    else:
        sql = "SELECT p.intprojid "
    sql += "FROM mtp_projects_dissolve as p, %s s " % (layer)
    sql += "WHERE ST_DWithin(p.wkb_geometry, s.wkb_geometry, %s)" % (searchradius)

    cur.execute(sql)
    res=cur.fetchall()
    results = []
    for x in res:
        results += x
    return(results)

def multiOrSect(cur, searchradius, *layers):
    results = []
    for layer in layers:
        results += intersect(cur, searchradius, layer)
    return(results)

def multiAndSect(cur, searchradius, *layers):
    results = intersect(cur, searchradius, layers[0])
    for layer in layers[1:]:
        results = set.intersection(*map(results,intersect(cur, searchradius, layer)))
    return(results)

def notProj(cur, projectlist):
    # identify projects NOT satisfying the condition
    cur.execute("""SELECT DISTINCT intprojid FROM mtp_projects_dissolve""")
    res=cur.fetchall()
    allproj = []
    for x in res:
        allproj += x

    inverse = list(set(allproj) - set(projectlist))
    return(inverse)


initime = time.time()

def processSpatial():
    # DB Connection
    conn = db.connect("host='localhost' dbname='prioritization' user='postgres'")
    # creating a Cursor
    cur = conn.cursor()


    # Stuff a dictionary with our results. The key is the question ID.
    results = {}

    # 98 - Air quality: is the project on a freight route?
    print("Processing 98")
    results['q98'] = multiOrSect(cur, "10", "truck_bottlenecks_20120620", "t1t2")

    # 111 - Air quality: is the project within 1/4 mi buffer of schools?
    print("Processing 111")
    results['q111'] = multiOrSect(cur, "1320", "school_location2")

    # 69 - Is the project on an identified truck bottleneck?
    print("Processing 69")
    results['q69'] = multiOrSect(cur, "10", "truck_bottlenecks_20120620")

    # 72 - Is the project in an MIC? Does it connect two MICs or a MIC and an RGC?
    # NOTE: The first condition supercedes the remainder. Suspect this isn't what is wanted
    #print("Processing 72")
    #results['q72'] = multiOrSect(cur, "10", "micen")

    # 73 - Is the project in an MIC?
    # NOTE: This and 72 are dupes
    print("Processing 73")
    results['q73'] = multiOrSect(cur, "10", "micen")

    # 74 - Is the project within a TAZ identified as a freight generator
    print("Processing 74")
    results['q74'] = multiOrSect(cur, "10", "freight_gen_taz2010")

    # 114 - Is the project on a T1/T2 route
    print("Processing 114")
    results['q114'] = multiOrSect(cur, "10", "t1t2")


    # 66 - Within identified areas (18 jobs/acre density AND zoning)
    #print("Processing 66")
    #results['q66'] = multiAndSect(cur, "10", "all_jobs_18_lyr", "")

    # 67 - Within identified areas (18 jobs/acre density)
    print("Processing 67")
    results['q67'] = multiOrSect(cur, "10", "all_jobs_18_lyr")

    # 68 - Within identified areas (15 jobs/acre density; cluster employment)
    print("Processing 68")
    results['q68'] = multiOrSect(cur, "10", "cluster_15_lyr")

    # 106 - Within identified areas (15 jobs/acre density; family-wage)
    print("Processing 106")
    results['q106'] = multiOrSect(cur, "10", "fmlywg_15_lyr")

    # 107 - Within some reasonable distance (1/4mi?) from identified economic foundation points
    print("Processing 107")
    results['q107'] = multiOrSect(cur, "1320", "economic_foundations")


    # 116 - On the regional bicycle network
    print("Processing 116")
    results['q116'] = multiOrSect(cur, "10", "regional_bicycle_network")

    # 120 - Within 1/4mi of MTS transit stops
    print("Processing 120")
    results['q120'] = multiOrSect(cur, "1320", "regional_transit")

    # 101 - Project is in a critical area
    print("Processing 101")
    results['q101'] = notProj(cur, multiOrSect(cur, "10", "caoall"))

    # 122 - Project is within identified resource lands
    results['q122'] = multiOrSect(cur, "10", "reszone08_region")

    # 89 - On a facility with fatality, injury, or property damage incidents
    print("Processing 89")
    results['q89'] = multiOrSect(cur, "10", "all_collisions")

    # 141 - On security recovery annex facility
    print("Processing 141")
    results['q141'] = multiOrSect(cur, "10", "security")

    # 93 - In special needs area (NOTE: need guidance)

    # 150 - Connects area of low and very low to high or very high opp index

    # 151 - Connects to an area of low or very low

    # 152 - Connects to an area of high or very high

    # 59 - Within an RGC
    print("Processing 59")
    results['q59'] = multiOrSect(cur, "10", "urbcen")

    # 60 - Connect an RGC to RGC or MIC

    # 61 - Connect to RGC (5 mi)

    # 62 - In an area with housing density > 15
    print("Processing 62")
    results['q62'] = multiOrSect(cur, "10", "housing_density_15_plus")

    # 63 - In an area with housing density > 8
    print("Processing 63")
    results['q63'] = multiOrSect(cur, "10", "housing_density_8_to_15")


    # 75 - On an identified facility (bottleneck, chokepoint, congestion, etc.
    print("Processing 75")
    results['q75'] = multiOrSect(cur, "10", "chokepoints_and_bottlenecks", "congested_transit_corridors", "its_key_arterial_corridors")



def main():
    results = processSpatial()


if __name__ == "__main__":
     main()
