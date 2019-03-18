import urllib2
from BeautifulSoup import *
from urlparse import urljoin
import sqlite3 as sqlite
from BeautifulSoup import BeautifulSoup

ignorewords = set(['the', 'of', 'is', 'to', 'and', 'it', 'in', 'a', 'i'])


class Crawler:

    def __init__(self, dbname):
        self.con = sqlite.connect(dbname)

    def __del__(self):
        self.con.close()

    def dbcommit(self):
        self.con.commit()

    def addtoindex(self, url, soup):
        if self.isindexed(url):
            return
        print 'Indexing %s' % url
        text = self.gettextonly(soup)
        words = self.separatewords(text)

        urlid = self.getentryid('urllist', 'url', url)

        for i in range(len(words)):
            word = words[i]
            if word in ignorewords:
                continue
            wordid = self.getentryid('wordlist', 'word', word)
            self.con.execute("insert into wordlocation(urlid, wordid, location) values(%d,%d,%d)" % (urlid, wordid, i))

    def isindexed(self, url):
        u = self.con.execute("select rowid from urllist where url='%s'" % url).fetchone()
        if u is not None:
            v = self.con.execute('select * from wordlocation where urlid=%d' % u[0]).fetchone()
            if v is not None:
                return True
        return False

    def getentryid(self, table, field, value, createnew=True):
        cur = self.con.execute(
            "select rowid from %s where %s='%s'" % (table, field, value))
        res = cur.fetchone()
        if res == None:
            cur = self.con.execute(
                "insert into %s (%s) values ('%s')" % (table, field, value))
            return cur.lastrowid
        else:
            return res[0]

    def gettextonly(self, soup):
        v = soup.string
        if v is None:
            c = soup.contents
            resultext = ''
            for t in c:
                subtext = self.gettextonly(t)
                resultext += subtext + '\n'
            return resultext
        else:
            return v.strip()

    def addlinkref(self, urlFrom, urlTo, linkText):
        pass

    def separatewords(self, text):
        splitter = re.compile('\\W*')
        return [s.lower() for s in splitter.split(text) if s != '']

    def crawl(self, pages, depth=2):
        for i in range(depth):
            newpages = set()
            for page in pages:
                try:
                    c = urllib2.urlopen(page)
                except:
                    print "could not open %s" % page
                soup = BeautifulSoup(c.read())  # type: BeautifulSoup
                self.addtoindex(page, soup)

                links = soup('a')
                for link in links:
                    if ('href' in dict(link.attrs)):
                        url = urljoin(page, link['href'])
                        if url.find("'") != -1: continue
                        url = url.split('#')[0]
                        if url[0:4] == 'http' and not self.isindexed(url):
                            newpages.add(url)
                            linkText = self.gettextonly(link)
                            self.addlinkref(page, url, linkText)
                self.dbcommit()
            pages = newpages

    def createindextables(self):
        self.con.execute('create table urllist(url)')
        self.con.execute('create table wordlist(word)')
        self.con.execute('create table wordlocation(urlid,wordid,location)')
        self.con.execute('create table link(fromid integer,toid integer)')
        self.con.execute('create table linkwords(wordid, linkid)')
        self.con.execute('create index wordidx on wordlist(word)')
        self.con.execute('create index urlidx on urllist(url)')
        self.con.execute('create index wordidx on wordlocation(wordid)')
        self.con.execute('create index urltoidx on link(toid)')
        self.con.execute('create index urlfromidx on link(fromid)')
        self.dbcommit()


class Searcher:

    def __init__(self, dbname):
        self.con = sqlite.connect(dbname)

    def __del__(self):
        self.con.close()

    def getmatchrows(self, q):

        fieldlist = 'w0.urlid'
        tablelist = ''
        clauselist = ''
        wordids = []

        words = q.split(' ')
        tablenumber = 0

        for word in words:
            wordrow = self.con.execute("select rowid from wordlist where word='%s'" % word).fetchone()
            if wordrow is not None:
                wordid = wordrow[0]
                wordids.append(wordid)
                if tablenumber>0:
                    tablelist =','
                    clauselist += ' and '
                    clauselist += 'w%d.urlid=w%d.urlid and ' % (tablenumber - 1, tablenumber)
                fieldlist += ',w%d.location' % tablenumber
                tablelist += 'wordlocation w%d' % tablenumber
                clauselist += 'w%d.wordid=%d' % (tablenumber, wordid)
                tablenumber += 1

            else:
                return "could not find any matching document"

            # Create the query from the separate parts
            fullquery = 'select %s from %s where %s' % (fieldlist, tablelist, clauselist)
            cur = self.con.execute(fullquery)
            print cur.fetchone()[0]
            rows = [row for row in cur]
            return rows, wordids

    def getscoredlist(self, rows, wordids):
        totalscores = dict([(row[0], 0) for row in rows])
        # This is where you'll later put the scoring functions
        weights = []
        for (weight, scores) in weights:
            for url in totalscores:
                totalscores[url] += weight * scores[url]
        return totalscores

    def geturlname(self, id):
        return self.con.execute("select url from urllist where rowid=%d" % id).fetchone()[0]

    def query(self, q):
        rows, wordids = self.getmatchrows(q)
        scores = self.getscoredlist(rows, wordids)
        rankedscores = sorted([(score, url) for (url, score) in scores.items()], reverse=1)
        for (score, urlid) in rankedscores[0:10]:
            print '%f\t%s' % (score, self.geturlname(urlid))


crawler = Crawler('searchengine.db')
pagelist = ["https://en.wikipedia.org/wiki/Narendra_Modi"]
#crawler.crawl(pagelist)

#rows = [row for row in crawler.con.execute('select * from linkwords  limit 10')]
#print rows

e = Searcher('searchengine.db') # to search for a query
print e.query('friends')
