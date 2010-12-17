function setHeight(pageName)
{
	

	var minHeights = new Array();
	
	minHeights['home']=1040;
	minHeights['developers']=4040;
	minHeights['team']=1040;
	minHeights['source_code']=1040;

	if(screen.height < minHeights[pageName])
	{
		document.getElementById('content').style.height=''+minHeights[pageName]+'px';
	}
	else
	{
			document.getElementById('content').style.height=screen.height+'px';
	}
	
}
