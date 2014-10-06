/*
 Highcharts JS v4.0.4 (2014-09-02)

 (c) 2009-2014 Torstein Honsi

 License: www.highcharts.com/license
*/
(function(l,C){function K(a,b,c){this.init.call(this,a,b,c)}var P=l.arrayMin,Q=l.arrayMax,s=l.each,F=l.extend,q=l.merge,R=l.map,o=l.pick,x=l.pInt,p=l.getOptions().plotOptions,g=l.seriesTypes,v=l.extendClass,L=l.splat,r=l.wrap,M=l.Axis,y=l.Tick,H=l.Point,S=l.Pointer,T=l.CenteredSeriesMixin,z=l.TrackerMixin,t=l.Series,w=Math,D=w.round,A=w.floor,N=w.max,U=l.Color,u=function(){};F(K.prototype,{init:function(a,b,c){var d=this,e=d.defaultOptions;d.chart=b;if(b.angular)e.background={};d.options=a=q(e,a);
(a=a.background)&&s([].concat(L(a)).reverse(),function(a){var b=a.backgroundColor,a=q(d.defaultBackgroundOptions,a);if(b)a.backgroundColor=b;a.color=a.backgroundColor;c.options.plotBands.unshift(a)})},defaultOptions:{center:["50%","50%"],size:"85%",startAngle:0},defaultBackgroundOptions:{shape:"circle",borderWidth:1,borderColor:"silver",backgroundColor:{linearGradient:{x1:0,y1:0,x2:0,y2:1},stops:[[0,"#FFF"],[1,"#DDD"]]},from:-Number.MAX_VALUE,innerRadius:0,to:Number.MAX_VALUE,outerRadius:"105%"}});
var G=M.prototype,y=y.prototype,V={getOffset:u,redraw:function(){this.isDirty=!1},render:function(){this.isDirty=!1},setScale:u,setCategories:u,setTitle:u},O={isRadial:!0,defaultRadialGaugeOptions:{labels:{align:"center",x:0,y:null},minorGridLineWidth:0,minorTickInterval:"auto",minorTickLength:10,minorTickPosition:"inside",minorTickWidth:1,tickLength:10,tickPosition:"inside",tickWidth:2,title:{rotation:0},zIndex:2},defaultRadialXOptions:{gridLineWidth:1,labels:{align:null,distance:15,x:0,y:null},
maxPadding:0,minPadding:0,showLastLabel:!1,tickLength:0},defaultRadialYOptions:{gridLineInterpolation:"circle",labels:{align:"right",x:-3,y:-2},showLastLabel:!1,title:{x:4,text:null,rotation:90}},setOptions:function(a){a=this.options=q(this.defaultOptions,this.defaultRadialOptions,a);if(!a.plotBands)a.plotBands=[]},getOffset:function(){G.getOffset.call(this);this.chart.axisOffset[this.side]=0;this.center=this.pane.center=T.getCenter.call(this.pane)},getLinePath:function(a,b){var c=this.center,b=o(b,
c[2]/2-this.offset);return this.chart.renderer.symbols.arc(this.left+c[0],this.top+c[1],b,b,{start:this.startAngleRad,end:this.endAngleRad,open:!0,innerR:0})},setAxisTranslation:function(){G.setAxisTranslation.call(this);if(this.center)this.transA=this.isCircular?(this.endAngleRad-this.startAngleRad)/(this.max-this.min||1):this.center[2]/2/(this.max-this.min||1),this.minPixelPadding=this.isXAxis?this.transA*this.minPointOffset:0},beforeSetTickPositions:function(){this.autoConnect&&(this.max+=this.categories&&
1||this.pointRange||this.closestPointRange||0)},setAxisSize:function(){G.setAxisSize.call(this);if(this.isRadial){this.center=this.pane.center=l.CenteredSeriesMixin.getCenter.call(this.pane);if(this.isCircular)this.sector=this.endAngleRad-this.startAngleRad;this.len=this.width=this.height=this.center[2]*o(this.sector,1)/2}},getPosition:function(a,b){return this.postTranslate(this.isCircular?this.translate(a):0,o(this.isCircular?b:this.translate(a),this.center[2]/2)-this.offset)},postTranslate:function(a,
b){var c=this.chart,d=this.center,a=this.startAngleRad+a;return{x:c.plotLeft+d[0]+Math.cos(a)*b,y:c.plotTop+d[1]+Math.sin(a)*b}},getPlotBandPath:function(a,b,c){var d=this.center,e=this.startAngleRad,f=d[2]/2,h=[o(c.outerRadius,"100%"),c.innerRadius,o(c.thickness,10)],j=/%$/,k,m=this.isCircular;this.options.gridLineInterpolation==="polygon"?d=this.getPlotLinePath(a).concat(this.getPlotLinePath(b,!0)):(m||(h[0]=this.translate(a),h[1]=this.translate(b)),h=R(h,function(a){j.test(a)&&(a=x(a,10)*f/100);
return a}),c.shape==="circle"||!m?(a=-Math.PI/2,b=Math.PI*1.5,k=!0):(a=e+this.translate(a),b=e+this.translate(b)),d=this.chart.renderer.symbols.arc(this.left+d[0],this.top+d[1],h[0],h[0],{start:a,end:b,innerR:o(h[1],h[0]-h[2]),open:k}));return d},getPlotLinePath:function(a,b){var c=this,d=c.center,e=c.chart,f=c.getPosition(a),h,j,k;c.isCircular?k=["M",d[0]+e.plotLeft,d[1]+e.plotTop,"L",f.x,f.y]:c.options.gridLineInterpolation==="circle"?(a=c.translate(a))&&(k=c.getLinePath(0,a)):(s(e.xAxis,function(a){a.pane===
c.pane&&(h=a)}),k=[],a=c.translate(a),d=h.tickPositions,h.autoConnect&&(d=d.concat([d[0]])),b&&(d=[].concat(d).reverse()),s(d,function(f,c){j=h.getPosition(f,a);k.push(c?"L":"M",j.x,j.y)}));return k},getTitlePosition:function(){var a=this.center,b=this.chart,c=this.options.title;return{x:b.plotLeft+a[0]+(c.x||0),y:b.plotTop+a[1]-{high:0.5,middle:0.25,low:0}[c.align]*a[2]+(c.y||0)}}};r(G,"init",function(a,b,c){var i;var d=b.angular,e=b.polar,f=c.isX,h=d&&f,j,k;k=b.options;var m=c.pane||0;if(d){if(F(this,
h?V:O),j=!f)this.defaultRadialOptions=this.defaultRadialGaugeOptions}else if(e)F(this,O),this.defaultRadialOptions=(j=f)?this.defaultRadialXOptions:q(this.defaultYAxisOptions,this.defaultRadialYOptions);a.call(this,b,c);if(!h&&(d||e)){a=this.options;if(!b.panes)b.panes=[];this.pane=(i=b.panes[m]=b.panes[m]||new K(L(k.pane)[m],b,this),m=i);m=m.options;b.inverted=!1;k.chart.zoomType=null;this.startAngleRad=b=(m.startAngle-90)*Math.PI/180;this.endAngleRad=k=(o(m.endAngle,m.startAngle+360)-90)*Math.PI/
180;this.offset=a.offset||0;if((this.isCircular=j)&&c.max===C&&k-b===2*Math.PI)this.autoConnect=!0}});r(y,"getPosition",function(a,b,c,d,e){var f=this.axis;return f.getPosition?f.getPosition(c):a.call(this,b,c,d,e)});r(y,"getLabelPosition",function(a,b,c,d,e,f,h,j,k){var m=this.axis,i=f.y,n=f.align,g=(m.translate(this.pos)+m.startAngleRad+Math.PI/2)/Math.PI*180%360;m.isRadial?(a=m.getPosition(this.pos,m.center[2]/2+o(f.distance,-25)),f.rotation==="auto"?d.attr({rotation:g}):i===null&&(i=m.chart.renderer.fontMetrics(d.styles.fontSize).b-
d.getBBox().height/2),n===null&&(n=m.isCircular?g>20&&g<160?"left":g>200&&g<340?"right":"center":"center",d.attr({align:n})),a.x+=f.x,a.y+=i):a=a.call(this,b,c,d,e,f,h,j,k);return a});r(y,"getMarkPath",function(a,b,c,d,e,f,h){var j=this.axis;j.isRadial?(a=j.getPosition(this.pos,j.center[2]/2+d),b=["M",b,c,"L",a.x,a.y]):b=a.call(this,b,c,d,e,f,h);return b});p.arearange=q(p.area,{lineWidth:1,marker:null,threshold:null,tooltip:{pointFormat:'<span style="color:{series.color}">●</span> {series.name}: <b>{point.low}</b> - <b>{point.high}</b><br/>'},
trackByArea:!0,dataLabels:{align:null,verticalAlign:null,xLow:0,xHigh:0,yLow:0,yHigh:0},states:{hover:{halo:!1}}});g.arearange=v(g.area,{type:"arearange",pointArrayMap:["low","high"],toYData:function(a){return[a.low,a.high]},pointValKey:"low",getSegments:function(){var a=this;s(a.points,function(b){if(!a.options.connectNulls&&(b.low===null||b.high===null))b.y=null;else if(b.low===null&&b.high!==null)b.y=b.high});t.prototype.getSegments.call(this)},translate:function(){var a=this.yAxis;g.area.prototype.translate.apply(this);
s(this.points,function(b){var c=b.low,d=b.high,e=b.plotY;d===null&&c===null?b.y=null:c===null?(b.plotLow=b.plotY=null,b.plotHigh=a.translate(d,0,1,0,1)):d===null?(b.plotLow=e,b.plotHigh=null):(b.plotLow=e,b.plotHigh=a.translate(d,0,1,0,1))})},getSegmentPath:function(a){var b,c=[],d=a.length,e=t.prototype.getSegmentPath,f,h;h=this.options;var j=h.step;for(b=HighchartsAdapter.grep(a,function(a){return a.plotLow!==null});d--;)f=a[d],f.plotHigh!==null&&c.push({plotX:f.plotX,plotY:f.plotHigh});a=e.call(this,
b);if(j)j===!0&&(j="left"),h.step={left:"right",center:"center",right:"left"}[j];c=e.call(this,c);h.step=j;h=[].concat(a,c);c[0]="L";this.areaPath=this.areaPath.concat(a,c);return h},drawDataLabels:function(){var a=this.data,b=a.length,c,d=[],e=t.prototype,f=this.options.dataLabels,h=f.align,j,k=this.chart.inverted;if(f.enabled||this._hasPointLabels){for(c=b;c--;)if(j=a[c],j.y=j.high,j._plotY=j.plotY,j.plotY=j.plotHigh,d[c]=j.dataLabel,j.dataLabel=j.dataLabelUpper,j.below=!1,k){if(!h)f.align="left";
f.x=f.xHigh}else f.y=f.yHigh;e.drawDataLabels&&e.drawDataLabels.apply(this,arguments);for(c=b;c--;)if(j=a[c],j.dataLabelUpper=j.dataLabel,j.dataLabel=d[c],j.y=j.low,j.plotY=j._plotY,j.below=!0,k){if(!h)f.align="right";f.x=f.xLow}else f.y=f.yLow;e.drawDataLabels&&e.drawDataLabels.apply(this,arguments)}f.align=h},alignDataLabel:function(){g.column.prototype.alignDataLabel.apply(this,arguments)},getSymbol:u,drawPoints:u});p.areasplinerange=q(p.arearange);g.areasplinerange=v(g.arearange,{type:"areasplinerange",
getPointSpline:g.spline.prototype.getPointSpline});(function(){var a=g.column.prototype;p.columnrange=q(p.column,p.arearange,{lineWidth:1,pointRange:null});g.columnrange=v(g.arearange,{type:"columnrange",translate:function(){var b=this,c=b.yAxis,d;a.translate.apply(b);s(b.points,function(a){var f=a.shapeArgs,h=b.options.minPointLength,j;a.tooltipPos=null;a.plotHigh=d=c.translate(a.high,0,1,0,1);a.plotLow=a.plotY;j=d;a=a.plotY-d;a<h&&(h-=a,a+=h,j-=h/2);f.height=a;f.y=j})},trackerGroups:["group","dataLabelsGroup"],
drawGraph:u,pointAttrToOptions:a.pointAttrToOptions,drawPoints:a.drawPoints,drawTracker:a.drawTracker,animate:a.animate,getColumnMetrics:a.getColumnMetrics})})();p.gauge=q(p.line,{dataLabels:{enabled:!0,defer:!1,y:15,borderWidth:1,borderColor:"silver",borderRadius:3,crop:!1,style:{fontWeight:"bold"},verticalAlign:"top",zIndex:2},dial:{},pivot:{},tooltip:{headerFormat:""},showInLegend:!1});z={type:"gauge",pointClass:v(H,{setState:function(a){this.state=a}}),angular:!0,drawGraph:u,fixedBox:!0,forceDL:!0,
trackerGroups:["group","dataLabelsGroup"],translate:function(){var a=this.yAxis,b=this.options,c=a.center;this.generatePoints();s(this.points,function(d){var e=q(b.dial,d.dial),f=x(o(e.radius,80))*c[2]/200,h=x(o(e.baseLength,70))*f/100,j=x(o(e.rearLength,10))*f/100,k=e.baseWidth||3,m=e.topWidth||1,i=b.overshoot,n=a.startAngleRad+a.translate(d.y,null,null,null,!0);i&&typeof i==="number"?(i=i/180*Math.PI,n=Math.max(a.startAngleRad-i,Math.min(a.endAngleRad+i,n))):b.wrap===!1&&(n=Math.max(a.startAngleRad,
Math.min(a.endAngleRad,n)));n=n*180/Math.PI;d.shapeType="path";d.shapeArgs={d:e.path||["M",-j,-k/2,"L",h,-k/2,f,-m/2,f,m/2,h,k/2,-j,k/2,"z"],translateX:c[0],translateY:c[1],rotation:n};d.plotX=c[0];d.plotY=c[1]})},drawPoints:function(){var a=this,b=a.yAxis.center,c=a.pivot,d=a.options,e=d.pivot,f=a.chart.renderer;s(a.points,function(c){var b=c.graphic,k=c.shapeArgs,e=k.d,i=q(d.dial,c.dial);b?(b.animate(k),k.d=e):c.graphic=f[c.shapeType](k).attr({stroke:i.borderColor||"none","stroke-width":i.borderWidth||
0,fill:i.backgroundColor||"black",rotation:k.rotation}).add(a.group)});c?c.animate({translateX:b[0],translateY:b[1]}):a.pivot=f.circle(0,0,o(e.radius,5)).attr({"stroke-width":e.borderWidth||0,stroke:e.borderColor||"silver",fill:e.backgroundColor||"black"}).translate(b[0],b[1]).add(a.group)},animate:function(a){var b=this;if(!a)s(b.points,function(a){var d=a.graphic;d&&(d.attr({rotation:b.yAxis.startAngleRad*180/Math.PI}),d.animate({rotation:a.shapeArgs.rotation},b.options.animation))}),b.animate=
null},render:function(){this.group=this.plotGroup("group","series",this.visible?"visible":"hidden",this.options.zIndex,this.chart.seriesGroup);t.prototype.render.call(this);this.group.clip(this.chart.clipRect)},setData:function(a,b){t.prototype.setData.call(this,a,!1);this.processData();this.generatePoints();o(b,!0)&&this.chart.redraw()},drawTracker:z&&z.drawTrackerPoint};g.gauge=v(g.line,z);p.boxplot=q(p.column,{fillColor:"#FFFFFF",lineWidth:1,medianWidth:2,states:{hover:{brightness:-0.3}},threshold:null,
tooltip:{pointFormat:'<span style="color:{series.color}">●</span> <b> {series.name}</b><br/>Maximum: {point.high}<br/>Upper quartile: {point.q3}<br/>Median: {point.median}<br/>Lower quartile: {point.q1}<br/>Minimum: {point.low}<br/>'},whiskerLength:"50%",whiskerWidth:2});g.boxplot=v(g.column,{type:"boxplot",pointArrayMap:["low","q1","median","q3","high"],toYData:function(a){return[a.low,a.q1,a.median,a.q3,a.high]},pointValKey:"high",pointAttrToOptions:{fill:"fillColor",stroke:"color","stroke-width":"lineWidth"},
drawDataLabels:u,translate:function(){var a=this.yAxis,b=this.pointArrayMap;g.column.prototype.translate.apply(this);s(this.points,function(c){s(b,function(b){c[b]!==null&&(c[b+"Plot"]=a.translate(c[b],0,1,0,1))})})},drawPoints:function(){var a=this,b=a.points,c=a.options,d=a.chart.renderer,e,f,h,j,k,m,i,n,g,l,p,I,r,q,J,u,v,t,w,x,z,y,E=a.doQuartiles!==!1,B=parseInt(a.options.whiskerLength,10)/100;s(b,function(b){g=b.graphic;z=b.shapeArgs;p={};q={};u={};y=b.color||a.color;if(b.plotY!==C)if(e=b.pointAttr[b.selected?
"selected":""],v=z.width,t=A(z.x),w=t+v,x=D(v/2),f=A(E?b.q1Plot:b.lowPlot),h=A(E?b.q3Plot:b.lowPlot),j=A(b.highPlot),k=A(b.lowPlot),p.stroke=b.stemColor||c.stemColor||y,p["stroke-width"]=o(b.stemWidth,c.stemWidth,c.lineWidth),p.dashstyle=b.stemDashStyle||c.stemDashStyle,q.stroke=b.whiskerColor||c.whiskerColor||y,q["stroke-width"]=o(b.whiskerWidth,c.whiskerWidth,c.lineWidth),u.stroke=b.medianColor||c.medianColor||y,u["stroke-width"]=o(b.medianWidth,c.medianWidth,c.lineWidth),u["stroke-linecap"]="round",
i=p["stroke-width"]%2/2,n=t+x+i,l=["M",n,h,"L",n,j,"M",n,f,"L",n,k],E&&(i=e["stroke-width"]%2/2,n=A(n)+i,f=A(f)+i,h=A(h)+i,t+=i,w+=i,I=["M",t,h,"L",t,f,"L",w,f,"L",w,h,"L",t,h,"z"]),B&&(i=q["stroke-width"]%2/2,j+=i,k+=i,r=["M",n-x*B,j,"L",n+x*B,j,"M",n-x*B,k,"L",n+x*B,k]),i=u["stroke-width"]%2/2,m=D(b.medianPlot)+i,J=["M",t,m,"L",w,m],g)b.stem.animate({d:l}),B&&b.whiskers.animate({d:r}),E&&b.box.animate({d:I}),b.medianShape.animate({d:J});else{b.graphic=g=d.g().add(a.group);b.stem=d.path(l).attr(p).add(g);
if(B)b.whiskers=d.path(r).attr(q).add(g);if(E)b.box=d.path(I).attr(e).add(g);b.medianShape=d.path(J).attr(u).add(g)}})}});p.errorbar=q(p.boxplot,{color:"#000000",grouping:!1,linkedTo:":previous",tooltip:{pointFormat:'<span style="color:{series.color}">●</span> {series.name}: <b>{point.low}</b> - <b>{point.high}</b><br/>'},whiskerWidth:null});g.errorbar=v(g.boxplot,{type:"errorbar",pointArrayMap:["low","high"],toYData:function(a){return[a.low,a.high]},pointValKey:"high",doQuartiles:!1,drawDataLabels:g.arearange?
g.arearange.prototype.drawDataLabels:u,getColumnMetrics:function(){return this.linkedParent&&this.linkedParent.columnMetrics||g.column.prototype.getColumnMetrics.call(this)}});p.waterfall=q(p.column,{lineWidth:1,lineColor:"#333",dashStyle:"dot",borderColor:"#333",states:{hover:{lineWidthPlus:0}}});g.waterfall=v(g.column,{type:"waterfall",upColorProp:"fill",pointArrayMap:["low","y"],pointValKey:"y",init:function(a,b){b.stacking=!0;g.column.prototype.init.call(this,a,b)},translate:function(){var a=
this.yAxis,b,c,d,e,f,h,j,k,m,i;b=this.options.threshold;g.column.prototype.translate.apply(this);k=m=b;d=this.points;for(c=0,b=d.length;c<b;c++){e=d[c];f=e.shapeArgs;h=this.getStack(c);i=h.points[this.index+","+c];if(isNaN(e.y))e.y=this.yData[c];j=N(k,k+e.y)+i[0];f.y=a.translate(j,0,1);e.isSum?(f.y=a.translate(i[1],0,1),f.height=a.translate(i[0],0,1)-f.y):e.isIntermediateSum?(f.y=a.translate(i[1],0,1),f.height=a.translate(m,0,1)-f.y,m=i[1]):k+=h.total;f.height<0&&(f.y+=f.height,f.height*=-1);e.plotY=
f.y=D(f.y)-this.borderWidth%2/2;f.height=N(D(f.height),0.001);e.yBottom=f.y+f.height;f=e.plotY+(e.negative?f.height:0);this.chart.inverted?e.tooltipPos[0]=a.len-f:e.tooltipPos[1]=f}},processData:function(a){var b=this.yData,c=this.points,d,e=b.length,f,h,j,k,m,i;h=f=j=k=this.options.threshold||0;for(i=0;i<e;i++)m=b[i],d=c&&c[i]?c[i]:{},m==="sum"||d.isSum?b[i]=h:m==="intermediateSum"||d.isIntermediateSum?b[i]=f:(h+=m,f+=m),j=Math.min(h,j),k=Math.max(h,k);t.prototype.processData.call(this,a);this.dataMin=
j;this.dataMax=k},toYData:function(a){if(a.isSum)return a.x===0?null:"sum";else if(a.isIntermediateSum)return a.x===0?null:"intermediateSum";return a.y},getAttribs:function(){g.column.prototype.getAttribs.apply(this,arguments);var a=this.options,b=a.states,c=a.upColor||this.color,a=l.Color(c).brighten(0.1).get(),d=q(this.pointAttr),e=this.upColorProp;d[""][e]=c;d.hover[e]=b.hover.upColor||a;d.select[e]=b.select.upColor||c;s(this.points,function(a){if(a.y>0&&!a.color)a.pointAttr=d,a.color=c})},getGraphPath:function(){var a=
this.data,b=a.length,c=D(this.options.lineWidth+this.borderWidth)%2/2,d=[],e,f,h;for(h=1;h<b;h++)f=a[h].shapeArgs,e=a[h-1].shapeArgs,f=["M",e.x+e.width,e.y+c,"L",f.x,e.y+c],a[h-1].y<0&&(f[2]+=e.height,f[5]+=e.height),d=d.concat(f);return d},getExtremes:u,getStack:function(a){var b=this.yAxis.stacks,c=this.stackKey;this.processedYData[a]<this.options.threshold&&(c="-"+c);return b[c][a]},drawGraph:t.prototype.drawGraph});p.bubble=q(p.scatter,{dataLabels:{formatter:function(){return this.point.z},inside:!0,
style:{color:"white",textShadow:"0px 0px 3px black"},verticalAlign:"middle"},marker:{lineColor:null,lineWidth:1},minSize:8,maxSize:"20%",states:{hover:{halo:{size:5}}},tooltip:{pointFormat:"({point.x}, {point.y}), Size: {point.z}"},turboThreshold:0,zThreshold:0});z=v(H,{haloPath:function(){return H.prototype.haloPath.call(this,this.shapeArgs.r+this.series.options.states.hover.halo.size)}});g.bubble=v(g.scatter,{type:"bubble",pointClass:z,pointArrayMap:["y","z"],parallelArrays:["x","y","z"],trackerGroups:["group",
"dataLabelsGroup"],bubblePadding:!0,pointAttrToOptions:{stroke:"lineColor","stroke-width":"lineWidth",fill:"fillColor"},applyOpacity:function(a){var b=this.options.marker,c=o(b.fillOpacity,0.5),a=a||b.fillColor||this.color;c!==1&&(a=U(a).setOpacity(c).get("rgba"));return a},convertAttribs:function(){var a=t.prototype.convertAttribs.apply(this,arguments);a.fill=this.applyOpacity(a.fill);return a},getRadii:function(a,b,c,d){var e,f,h,j=this.zData,k=[],m=this.options.sizeBy!=="width";for(f=0,e=j.length;f<
e;f++)h=b-a,h=h>0?(j[f]-a)/(b-a):0.5,m&&h>=0&&(h=Math.sqrt(h)),k.push(w.ceil(c+h*(d-c))/2);this.radii=k},animate:function(a){var b=this.options.animation;if(!a)s(this.points,function(a){var d=a.graphic,a=a.shapeArgs;d&&a&&(d.attr("r",1),d.animate({r:a.r},b))}),this.animate=null},translate:function(){var a,b=this.data,c,d,e=this.radii;g.scatter.prototype.translate.call(this);for(a=b.length;a--;)c=b[a],d=e?e[a]:0,c.negative=c.z<(this.options.zThreshold||0),d>=this.minPxSize/2?(c.shapeType="circle",
c.shapeArgs={x:c.plotX,y:c.plotY,r:d},c.dlBox={x:c.plotX-d,y:c.plotY-d,width:2*d,height:2*d}):c.shapeArgs=c.plotY=c.dlBox=C},drawLegendSymbol:function(a,b){var c=x(a.itemStyle.fontSize)/2;b.legendSymbol=this.chart.renderer.circle(c,a.baseline-c,c).attr({zIndex:3}).add(b.legendGroup);b.legendSymbol.isMarker=!0},drawPoints:g.column.prototype.drawPoints,alignDataLabel:g.column.prototype.alignDataLabel});M.prototype.beforePadding=function(){var a=this,b=this.len,c=this.chart,d=0,e=b,f=this.isXAxis,h=
f?"xData":"yData",j=this.min,k={},m=w.min(c.plotWidth,c.plotHeight),i=Number.MAX_VALUE,n=-Number.MAX_VALUE,g=this.max-j,l=b/g,p=[];this.tickPositions&&(s(this.series,function(b){var h=b.options;if(b.bubblePadding&&(b.visible||!c.options.chart.ignoreHiddenSeries))if(a.allowZoomOutside=!0,p.push(b),f)s(["minSize","maxSize"],function(a){var b=h[a],f=/%$/.test(b),b=x(b);k[a]=f?m*b/100:b}),b.minPxSize=k.minSize,b=b.zData,b.length&&(i=o(h.zMin,w.min(i,w.max(P(b),h.displayNegative===!1?h.zThreshold:-Number.MAX_VALUE))),
n=o(h.zMax,w.max(n,Q(b))))}),s(p,function(a){var b=a[h],c=b.length,m;f&&a.getRadii(i,n,k.minSize,k.maxSize);if(g>0)for(;c--;)typeof b[c]==="number"&&(m=a.radii[c],d=Math.min((b[c]-j)*l-m,d),e=Math.max((b[c]-j)*l+m,e))}),p.length&&g>0&&o(this.options.min,this.userMin)===C&&o(this.options.max,this.userMax)===C&&(e-=b,l*=(b+d-e)/b,this.min+=d/l,this.max+=e/l))};(function(){function a(a,b,c){a.call(this,b,c);if(this.chart.polar)this.closeSegment=function(a){var b=this.xAxis.center;a.push("L",b[0],b[1])},
this.closedStacks=!0}function b(a,b){var c=this.chart,d=this.options.animation,e=this.group,i=this.markerGroup,n=this.xAxis.center,g=c.plotLeft,l=c.plotTop;if(c.polar){if(c.renderer.isSVG)d===!0&&(d={}),b?(c={translateX:n[0]+g,translateY:n[1]+l,scaleX:0.001,scaleY:0.001},e.attr(c),i&&i.attr(c)):(c={translateX:g,translateY:l,scaleX:1,scaleY:1},e.animate(c,d),i&&i.animate(c,d),this.animate=null)}else a.call(this,b)}var c=t.prototype,d=S.prototype,e;c.toXY=function(a){var b,c=this.chart,d=a.plotX;b=
a.plotY;a.rectPlotX=d;a.rectPlotY=b;d=(d/Math.PI*180+this.xAxis.pane.options.startAngle)%360;d<0&&(d+=360);a.clientX=d;b=this.xAxis.postTranslate(a.plotX,this.yAxis.len-b);a.plotX=a.polarPlotX=b.x-c.plotLeft;a.plotY=a.polarPlotY=b.y-c.plotTop};c.orderTooltipPoints=function(a){if(this.chart.polar&&(a.sort(function(a,b){return a.clientX-b.clientX}),a[0]))a[0].wrappedClientX=a[0].clientX+360,a.push(a[0])};g.area&&r(g.area.prototype,"init",a);g.areaspline&&r(g.areaspline.prototype,"init",a);g.spline&&
r(g.spline.prototype,"getPointSpline",function(a,b,c,d){var e,i,n,g,l,p,o;if(this.chart.polar){e=c.plotX;i=c.plotY;a=b[d-1];n=b[d+1];this.connectEnds&&(a||(a=b[b.length-2]),n||(n=b[1]));if(a&&n)g=a.plotX,l=a.plotY,b=n.plotX,p=n.plotY,g=(1.5*e+g)/2.5,l=(1.5*i+l)/2.5,n=(1.5*e+b)/2.5,o=(1.5*i+p)/2.5,b=Math.sqrt(Math.pow(g-e,2)+Math.pow(l-i,2)),p=Math.sqrt(Math.pow(n-e,2)+Math.pow(o-i,2)),g=Math.atan2(l-i,g-e),l=Math.atan2(o-i,n-e),o=Math.PI/2+(g+l)/2,Math.abs(g-o)>Math.PI/2&&(o-=Math.PI),g=e+Math.cos(o)*
b,l=i+Math.sin(o)*b,n=e+Math.cos(Math.PI+o)*p,o=i+Math.sin(Math.PI+o)*p,c.rightContX=n,c.rightContY=o;d?(c=["C",a.rightContX||a.plotX,a.rightContY||a.plotY,g||e,l||i,e,i],a.rightContX=a.rightContY=null):c=["M",e,i]}else c=a.call(this,b,c,d);return c});r(c,"translate",function(a){a.call(this);if(this.chart.polar&&!this.preventPostTranslate)for(var a=this.points,b=a.length;b--;)this.toXY(a[b])});r(c,"getSegmentPath",function(a,b){var c=this.points;if(this.chart.polar&&this.options.connectEnds!==!1&&
b[b.length-1]===c[c.length-1]&&c[0].y!==null)this.connectEnds=!0,b=[].concat(b,[c[0]]);return a.call(this,b)});r(c,"animate",b);r(c,"setTooltipPoints",function(a,b){this.chart.polar&&F(this.xAxis,{tooltipLen:360});return a.call(this,b)});if(g.column)e=g.column.prototype,r(e,"animate",b),r(e,"translate",function(a){var b=this.xAxis,c=this.yAxis.len,d=b.center,e=b.startAngleRad,i=this.chart.renderer,g,l;this.preventPostTranslate=!0;a.call(this);if(b.isRadial){b=this.points;for(l=b.length;l--;)g=b[l],
a=g.barX+e,g.shapeType="path",g.shapeArgs={d:i.symbols.arc(d[0],d[1],c-g.plotY,null,{start:a,end:a+g.pointWidth,innerR:c-o(g.yBottom,c)})},this.toXY(g),g.tooltipPos=[g.plotX,g.plotY],g.ttBelow=g.plotY>d[1]}}),r(e,"alignDataLabel",function(a,b,d,e,g,i){if(this.chart.polar){a=b.rectPlotX/Math.PI*180;if(e.align===null)e.align=a>20&&a<160?"left":a>200&&a<340?"right":"center";if(e.verticalAlign===null)e.verticalAlign=a<45||a>315?"bottom":a>135&&a<225?"top":"middle";c.alignDataLabel.call(this,b,d,e,g,i)}else a.call(this,
b,d,e,g,i)});r(d,"getIndex",function(a,b){var c,d=this.chart,e;d.polar?(e=d.xAxis[0].center,c=b.chartX-e[0]-d.plotLeft,d=b.chartY-e[1]-d.plotTop,c=180-Math.round(Math.atan2(c,d)/Math.PI*180)):c=a.call(this,b);return c});r(d,"getCoordinates",function(a,b){var c=this.chart,d={xAxis:[],yAxis:[]};c.polar?s(c.axes,function(a){var e=a.isXAxis,f=a.center,g=b.chartX-f[0]-c.plotLeft,f=b.chartY-f[1]-c.plotTop;d[e?"xAxis":"yAxis"].push({axis:a,value:a.translate(e?Math.PI-Math.atan2(g,f):Math.sqrt(Math.pow(g,
2)+Math.pow(f,2)),!0)})}):d=a.call(this,b);return d})})()})(Highcharts);
