function setHeight(pageName)
{
	

	var minHeights = new Array();

	minHeights['home']=1040;
	minHeights['developers']=6640;
	minHeights['team']=1040;
	minHeights['source_code']=2440;

	if(screen.height < minHeights[pageName])
	{
		document.getElementById('content').style.height=''+minHeights[pageName]+'px';
	}
	else
	{
			document.getElementById('content').style.height=screen.height+'px';
	}
	
}
