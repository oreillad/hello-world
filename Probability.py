from sig.md.grid import GridFileRepository, Feeds, Ticks

os.system("Pause")
# _________________________________________
# LOAD THE DATA
date = "20160229"

repo = GridFileRepository(feed=Feeds.TopOfBook, site="FRAE")
data = repo.load("NGJ6 COM", date, version="3.8.2.120", flavour="QEDNY4")

trd = data.get(Ticks.Trd, status=True)
mkt = data.get(Ticks.Mkt, status=True)

print "Files opened"
# _________________________________________
# RUN THROUGH THE PACKAGES
j = 0
sequential_trd = 0
duplicate_trd_in_pkt = 0
duplicate_mkt_in_pkt = 0

for i in range(1, len(trd)):
   if trd.HATTimeOfEventInMilliseconds.iat[i] == trd.HATTimeOfEventInMilliseconds.iat[i - 1]:
       duplicate_trd_in_pkt += 1
       continue
   else:
       while mkt.HATTimeOfEventInMilliseconds.iat[j] <= trd.HATTimeOfEventInMilliseconds.iat[i - 1]:
           j += 1

           if mkt.HATTimeOfEventInMilliseconds.iat[j] == mkt.HATTimeOfEventInMilliseconds.iat[j - 1]:
               duplicate_mkt_in_pkt += 1

           if j == len(mkt):
               break

       if mkt.HATTimeOfEventInMilliseconds.iat[j] == trd.HATTimeOfEventInMilliseconds.iat[i]:
           sequential_trd += 1

while j < len(mkt):
   if mkt.HATTimeOfEventInMilliseconds.iat[j] == mkt.HATTimeOfEventInMilliseconds.iat[j - 1]:
       duplicate_mkt_in_pkt += 1
   j += 1

data_packages.append(len(trd) - duplicate_mkt_in_pkt)
split_probability.append(float(sequential_trd)/(len(mkt) - duplicate_mkt_in_pkt) * 100)

print "Number of Data Packages Containing Trd's: ", len(trd) - duplicate_trd_in_pkt
print "Number of Data Packages Containing Mkt's: ", len(mkt) - duplicate_mkt_in_pkt
print "Probability of a trd being split over a packet is: ", \
   float(sequential_trd)/(len(mkt) - duplicate_mkt_in_pkt) * 100, " %"
