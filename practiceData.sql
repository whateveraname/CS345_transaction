create table Company(cname varchar(30) primary key, country varchar(30));

insert into Company values('GizmoWorks','USA');
insert into Company values('Canon','Japan');
insert into Company values('Hitachi','Japan');
insert into Company values('BizWorks', NULL);
insert into Company values('MonkeyBiz', NULL);

create table Product(pname varchar(30) primary key, price float, category varchar(30), manufacturer varchar(30) references Company);

insert into Product values('Gizmo',19.99,'Gadgets','GizmoWorks');
insert into Product values('PowerGizmo',29.99,'Gadgets','GizmoWorks');
insert into Product values('SingleTouch',149.99,'Photography','Canon');
insert into Product values('Multitouch',203.99,'Household','Hitachi');
insert into Product values('SuperGizmo',49.99, 'Gadgets', 'Hitachi');
insert into Product values('Gizmo-Plus',NULL,'Gadgets','GizmoWorks');
insert into Product values('SingleTouch-Light',89.99,'Photography','Canon');
insert into Product values('SingleTouch++',79.99,'Photography','MonkeyBiz');

create table Sales(pname varchar(30) references Product, sold int, month varchar(10), primary key (pname, month));
    
insert into Sales values('Gizmo',2,'February');
insert into Sales values('Gizmo',1,'July');
insert into Sales values('Gizmo',2,'September');
insert into Sales values('SingleTouch',1,'July');
insert into Sales values('SingleTouch',2,'December');
insert into Sales values('SuperGizmo',3,'June');
insert into Sales values('SuperGizmo',1,'September');
insert into Sales values('SingleTouch++',5,'December');
insert into Sales values('SingleTouch-Light',1,'December');
insert into Sales values('PowerGizmo',2,'December');
insert into Sales values('Multitouch',1,'August');
insert into Sales values('Multitouch',2,'December');
insert into Sales values('SingleTouch-Light',1,'June');

    

create table Employees(empID integer primary key, name varchar(50), phone varchar(12), managerID integer);

insert into Employees values (1, 'John', '555-1234',5);
insert into Employees values (2, 'Alice', '555-4321',3);
insert into Employees values (3, 'Peter', '555-2314',5);
insert into Employees values (4, 'Cecilia', '555-3241',1);
insert into Employees values (5, 'James', '555-4231',NULL);

create table Projects(empID integer, project varchar(40));

insert into Projects values (1, 'Web archive') ;
insert into Projects values (1, 'Phone app');
insert into Projects values (3, 'Web archive');
insert into Projects values (2, 'Tools');
insert into Projects values (3, 'Game design');


create table R(a int);
create table S(a int);
create table T(a int);

insert into R values (3);
insert into R values (5);
insert into R values (7);

insert into S values (6);
insert into S values (7);
insert into S values (8);

insert into T values (0);
insert into T values (1);
insert into T values (2);
insert into T values (3);
insert into T values (4);
insert into T values (5);
insert into T values (6);
insert into T values (7);
insert into T values (8);
insert into T values (9);

