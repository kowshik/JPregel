function setHeight(pageName)
{
	

	var minHeights = new Array();
	
	minHeights['home']=690;
	minHeights['version_history']=1090;
	minHeights['installation']=3200;
	minHeights['screenshots']=1050;
	minHeights['source_code']=690;
	minHeights['authors_contact']=690;
	minHeights['contribute']=690;
	minHeights['license']=690;
	
	if(screen.height < minHeights[pageName])
	{
		document.getElementById('content').style.height=''+minHeights[pageName]+'px';
	}
	else
	{
			document.getElementById('content').style.height=screen.height+'px';
	}
	
}
