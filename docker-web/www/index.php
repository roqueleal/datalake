<?php
$conn = pg_connect('host=172.17.0.1 port=5432 dbname=twitter_database user=admin password=root');
if(!$conn)
{
	echo "An error occurred.\n";
	exit;
}
 
$sql="SELECT * FROM tweets_data";
 
$result = pg_query($conn, $sql);
if(!$result)
{
	echo "An error occurred.\n";
	exit;
}
 
echo("<table border=2><tr><td>index</td><td>time</td><td>followers</td><td>user_favorites_count</td><td>username</td><td>hour</td><td>minute</td><td>day</td><td>month</td><td>year</td><td>clean text</td><td>length text</td><td>positivity</td><td>negativity</td><td>neutral</td><td>compound</td><td>sentiment</td></tr>");
while ($line = pg_fetch_array($result, null, PGSQL_ASSOC)) {
    echo("<tr>");
    foreach ($line as $col_value => $row_value) {
        echo("<td>$row_value</td>");
    }
    echo("</tr>\n");
}
echo("</table>");
?>