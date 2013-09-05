BEGIN{
  OFS="\t"
  srand()
  for(I=0;I<1000000;I++){
    X=-180+360*rand()
    Y=-90+180*rand()
    print I,X,Y
  }
}
