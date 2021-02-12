# isbn-encoder

The function should check if the content in column _isbn_ is a [valid 13-digit isbn code](https://en.wikipedia.org/wiki/International_Standard_Book_Number) and create new rows for each part of the ISBN code.

#### Describe your solution
In my solution I first Split the field ISBN by each part according to the composition of the International Book Number standard, I created a different dataframe for each EAN Number, Group Number, Publisher and Title number , and after that I create an output dataframe with union all command of dataframes including the original.

Additional I validated the ISBN number by valid Length of the string or check if the ISBN number exists at all. 

**Advantages:**
Firstable, discard all the invalid ISBN numbers avoiding work innecesary about the splitting part.

**Disadvantages:**
I suppose there are more efficient ways to split a valid field in only one step, avoiding the creation od too many dataframes

__TODO:__ 

The solution is describe as :

1 - Check the validity of the ISBN number by
    1.a ISBN number can't be null to continue else break the flow
    1.b ISBN number should have a valid length as standard explained 13 digits + 6 characters of the literal "ISBN: " , then when length( ISBN number) != 19 is a invalid number and break the flow, else continue
2 - Create 4 temporal dataframes Split the ISBN number of the dataframe input , getting in each dataframe the columns Name and Year that are fixed for each row on the output dataframe , split instructions as show below:
    2.a Extraction of EAN number with a Substring ISBN Number from 7th position , 3 characters
    2.b Extraction of Group Number with Substring ISBN Number from 11th position , 2 characters
    2.c Extraction of Publisher Number with a Substring ISBN Number from 14th position , 4 characters
    2.d Extraction of Tittle Number with Substring ISBN Number from 18th position , 3 characters
3 - Union all dataframes including the input one-> Input DF + Ean DF + Group DF + Publisher DF + Title DF
4 - Return output dataframe

### Example

#### Input

| Name        | Year           | ISBN  |
| ----------- |:--------------:|-------|
| EL NOMBRE DE LA ROSA      | 2015 | ISBN: 9788426403568 |

#### Output

| Name        | Year           | ISBN  |
| ----------- |:--------------:|-------|
| EL NOMBRE DE LA ROSA      | 2015 | ISBN: 9788426403568 |
| EL NOMBRE DE LA ROSA      | 2015 | ISBN-EAN: 978 |
| EL NOMBRE DE LA ROSA      | 2015 | ISBN-GROUP: 84 |
| EL NOMBRE DE LA ROSA      | 2015 | ISBN-PUBLISHER: 2640 |
| EL NOMBRE DE LA ROSA      | 2015 | ISBN-TITLE: 356 |
