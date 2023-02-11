Python script to simulate a car moving between two points on a map. 

The primary purpose of this script is to simulate a car moving along a route and sending its current location every n seconds to a central server. Think simulating 1000's of uber cars driving in a city. 

The script generates an OrderDict of points. The key is the timestamp, value is the (lat, long) tuple representing the location of the car at that point in time. Time is relative, with t=0 indicating the time when the journey begins.

This script requires a google maps api key. 

Getting Started
-----

```
virtualenv -p python3 venv
source venv/bin/activate
pip install -r requirements.txt
python vehicles.py <API Key> "HashedIn Technologies, Bangalore" "Cubbon Park, Bangalore"
```

The script will print two things - 
1. A list of (lat, long) along with a timestamp starting at 0
2. A Polyline for the route

You can use visualize the polyline using google's polyline utility - https://developers.google.com/maps/documentation/utilities/polylineutility. Paste the generated polyline and click "Decode Polyline".

Using it programmatically
---

To use this programmatically, call the function `get_points_along_path`. 

How it works
----

The script calls google maps directions api to get the directions from start point to end point. However, google maps does not give a smooth set of points. For some parts of the journey, you may have a large number of (lat, long) pairs, while for others it may just be a single point. 

So this script interpolates points and smoothens it so that you get a (lat, long) pair roughly every 5 seconds.
