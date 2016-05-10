--Register the jar
REGISTER /hirw-workshop/pig/wikipedia/PageRankPig-0.0.1.jar;
--Use PageRankLoad function with the LOAD operator
records = LOAD '/user/ubuntu/input/pagerank' USING com.hirw.pagerankpig.PageRankLoad() AS (page:chararray, links:bag{});
records = FILTER records BY page IS NOT NULL;
--Calculate the no of links and points per page
no_of_links_points_per_page = FOREACH records GENERATE page, COUNT(links) as no_of_links, (float)1/COUNT(links) as points;
page_link_mapping = FOREACH records GENERATE page as page, FLATTEN(links) as link;
--Remove any backlinks which does not have a page reference in the dataset
remove_links_not_pages = JOIN no_of_links_points_per_page BY page, page_link_mapping BY link;
only_good_pages = FOREACH remove_links_not_pages GENERATE page_link_mapping::page AS page, page_link_mapping::link as link;
--Join links to pages and group
joinResult = JOIN no_of_links_points_per_page BY page, only_good_pages BY page;
groupResult = GROUP joinResult BY only_good_pages::link;
result = FOREACH groupResult GENERATE group as page, SUM(joinResult.no_of_links_points_per_page::points) as rank;
ordered = ORDER result BY rank desc;
STORE ordered INTO 'output/pagerankpig';