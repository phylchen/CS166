//CURRENTLY:
//taken from lab 10 but needs revision to match with mechanic shop project

CREATE SEQUENCE part_number_seq  START WITH 50000;

CREATE LANGUAGE plpgsql;
CREATE OR REPLACE FUNCTION get_part_number()
 RETURNS "trigger" AS
 $BODY$
 BEGIN
   New.part_number:=nextval('part_number_seq');
   Return NEW;
 END;
 $BODY$
 LANGUAGE plpgsql VOLATILE;

 CREATE TRIGGER PartNumberTrigger
 BEFORE INSERT
 ON part_nyc
 FOR EACH ROW
 EXECUTE PROCEDURE get_part_number();
