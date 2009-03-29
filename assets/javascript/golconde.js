var config = '';
var json_data = '';

var keys = new Array('add','delete','errors','set','update');
var first = new Object();
var last_values = new Object();
var data_values = new Object();
var stop = false;

function jsonp_config(result)
{
  config = result.Destinations
  $.each(config, function(name, child){
    var dkey = name;
    
    // Set up our internal variables      
    first[dkey] = true;
    last_values[dkey] = new Object();
    data_values[dkey] = new Object();
    for ( var y = 0; y < keys.length; y++ )
    {
      last_values[dkey][keys[y]] = 0;
      data_values[dkey][keys[y]] = new Array();
    }     
    
    // Output our HTML additions
    $('#config').append('<li id="config_' + dkey + '"><a href"#' + dkey + '">Destination: ' + name + '</a></li>');  
    $('#config_' + dkey).append('<dl></dl>');
    $('#config_' + dkey + ' dl').append(
      '<dt>broker</dt><dd>' + child.stomp + '</dd>' + 
      '<dt>queue</dt><dd>' + child.queue + '</dd>' + 
      '<dt>pgsql</dt><dd>' + child.pgsql + '</dd>' +
      '<dt>target</dt><dd>' + child.target + '</dd>' + 
      '<dt>function</dt><dd>' + child.function + '</dd>');
    // Graph area
    $('#graphs').append('<li class="name"><a name="' + dkey + '">' + name + '</a></li>\n<li id="' + dkey + '" class="graph"></li>');  
    $('#config_' + dkey).append('<ul></ul>');   
    
    // Process the targets 
    p = $('#config_' + dkey + ' ul');
    $.each(child.Targets, function(tname, child){
      tkey = dkey + '_' + tname;

      // Setup our internal arrays
      first[tkey] = true;
      last_values[tkey] = new Object();
      data_values[tkey] = new Object();
      for ( var y = 0; y < keys.length; y++ )
      {
        last_values[tkey][keys[y]] = 0;
        data_values[tkey][keys[y]] = new Array();
      }     
      
      // Append the HTML      
      p.append('<li id="config_' + tkey + '"><a href="#' + tkey + '">Target: ' + tname + '</a></li>');         
      $('#config_' + tkey).append('<dl></dl>');
      $('#config_' + tkey + ' dl').append(
        '<dt>broker</dt><dd>' + child.stomp + '</dd>' + 
        '<dt>queue</dt><dd>' + child.queue + '</dd>' + 
        '<dt>pgsql</dt><dd>' + child.pgsql + '</dd>' +
        '<dt>target</dt><dd>' + child.target + '</dd>' + 
        '<dt>function</dt><dd>' + child.function + '</dd>');
      $('#graphs').append('<li class="name"><a name="' + tkey + '">' + name + ' - ' + tname + '</a></li>\n<li id="' + tkey + '" class="graph"></li>');  
    });
  });
  getData();
}

function init()
{
  $.ajax({
    dataType: 'jsonp',
    jsonp: 'plot',
    url: 'http://localhost:8000/config',});
}

function getData()
{
  $.ajax({
    dataType: 'jsonp',
    jsonp: 'plot',
    url: 'http://localhost:8000/stats/jsonp',
  });      
}      

function getTime()
{
   var currentTime = new Date();
   return currentTime.getTime();
}

function jsonp_stats(data)
{
  for ( var y = 0; y < data.threads.length; y++ )
  {
    $.each(data.threads[y], function(name, values){
      if (first[name] != true)
      {      
        for ( var z = 0; z < keys.length; z++ )
        {
          data_values[name][keys[z]].push([getTime(),(values[keys[z]] - last_values[name][keys[z]])]);
          if ( data_values[name][keys[z]].length > 300 ) data_values[name][keys[z]].shift();
        }
      }
      id = '#' + name;   
      $.plot($(id), [
        {data: data_values[name]['add'], label: 'Add',lines:{show:true}},
        {data: data_values[name]['delete'],label: 'Delete',lines:{show:true}},
        {data: data_values[name]['errors'],label: 'Errors',lines:{show:true}},
        {data: data_values[name]['set'], label: 'Set',lines:{show:true}},
        {data: data_values[name]['update'],label: 'Update',lines:{show:true}}
       ],{ borderWidth: '1px', xaxis: {mode: "time"}, yaxis: {label: 'Commands'}, legend: { position: "ne", borderWidth: '1px' }} 
      );
      if ( first[name] == true) first[name] = false;   
      for ( var z = 0; z < keys.length; z++ )
      {
        last_values[name][keys[z]] = values[keys[z]];
      }
    });
  }
 if ( stop != true ) setTimeout("getData()", 1000);
}