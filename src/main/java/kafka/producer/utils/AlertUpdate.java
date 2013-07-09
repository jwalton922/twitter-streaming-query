package kafka.producer.utils;

public class AlertUpdate {

    public class LatLonBox {
        public float lowerLat;
        public float lowerLon;
        public float upperLat;
        public float upperLon;

        public LatLonBox() {
            lowerLat = 0;
            lowerLon = 0;
            upperLat = 0;
            upperLon = 0;
        }
    }

    private String[] usernames;
    private String[] usernamesNoAtSigns;
    private String[] searchterms;
    private LatLonBox[] latLonBoxes;

    public AlertUpdate() {
        usernames = null;
        usernamesNoAtSigns = null;
        searchterms = null;
        latLonBoxes = null;
    }

    public AlertUpdate(String[] users, String[] terms, LatLonBox[] boxes) {
        usernames = users;
        usernamesNoAtSigns = getusernamesNoAtSigns();
        searchterms = terms;
        latLonBoxes = boxes;
    }

    public String[] getUserNames() {
        return usernames;
    }

    public String[] getHashtags() {
        return searchterms;
    }

    public LatLonBox[] getLatLonBoxes() {
        return latLonBoxes;
    }

    public double[][] getLocations() {
        double[][] back = null;
        if (latLonBoxes != null) {
            int numOfBoxes = latLonBoxes.length;
            back = new double[numOfBoxes * 2][2];
            int kk = 0; // Array row count - increment twice per box!
            for (int jj = 0; jj < numOfBoxes; jj++) {
                back[kk][0] = latLonBoxes[jj].lowerLon;
                back[kk][1] = latLonBoxes[jj].lowerLat;
                kk++;
                back[kk][0] = latLonBoxes[jj].upperLon;
                back[kk][1] = latLonBoxes[jj].upperLat;
                kk++;
            }
        }
        return back;
    }

    public String[] getusernamesNoAtSigns() {
        if (usernamesNoAtSigns == null && usernames != null) {
            usernamesNoAtSigns = new String[usernames.length];
            String tempString = null;
            int jj = 0;
            for (String user : usernames) {
                // remove the '@' and occassional white space
                tempString = user.replaceAll("@", "");
                tempString = tempString.replaceAll(" ", "");
                usernamesNoAtSigns[jj] = tempString;
                jj++;
            }
        }
        return usernamesNoAtSigns;
    }

}
