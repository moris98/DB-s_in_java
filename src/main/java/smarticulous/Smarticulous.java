package smarticulous;

import smarticulous.db.Exercise;
import smarticulous.db.Submission;
import smarticulous.db.User;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.io.File;


/**
 * The Smarticulous class, implementing a grading system.
 */
public class Smarticulous {

    /**
     * The connection to the underlying DB.
     * <p>
     * null if the db has not yet been opened.
     */
    Connection db;

    /**
     * Open the {@link Smarticulous} SQLite database.
     * <p>
     * This should open the database, creating a new one if necessary, and set the {@link #db} field
     * to the new connection.
     * <p>
     * The open method should make sure the database contains the following tables, creating them if necessary:
     *
     * <table>
     *   <caption><em>Table name: <strong>User</strong></em></caption>
     *   <tr><th>Column</th><th>Type</th></tr>
     *   <tr><td>UserId</td><td>Integer (Primary Key)</td></tr>
     *   <tr><td>Username</td><td>Text</td></tr>
     *   <tr><td>Firstname</td><td>Text</td></tr>
     *   <tr><td>Lastname</td><td>Text</td></tr>
     *   <tr><td>Password</td><td>Text</td></tr>
     * </table>
     *
     * <p>
     * <table>
     *   <caption><em>Table name: <strong>Exercise</strong></em></caption>
     *   <tr><th>Column</th><th>Type</th></tr>
     *   <tr><td>ExerciseId</td><td>Integer (Primary Key)</td></tr>
     *   <tr><td>Name</td><td>Text</td></tr>
     *   <tr><td>DueDate</td><td>Integer</td></tr>
     * </table>
     *
     * <p>
     * <table>
     *   <caption><em>Table name: <strong>Question</strong></em></caption>
     *   <tr><th>Column</th><th>Type</th></tr>
     *   <tr><td>ExerciseId</td><td>Integer</td></tr>
     *   <tr><td>QuestionId</td><td>Integer</td></tr>
     *   <tr><td>Name</td><td>Text</td></tr>
     *   <tr><td>Desc</td><td>Text</td></tr>
     *   <tr><td>Points</td><td>Integer</td></tr>
     * </table>
     * In this table the combination of ExerciseId and QuestionId together comprise the primary key.
     *
     * <p>
     * <table>
     *   <caption><em>Table name: <strong>Submission</strong></em></caption>
     *   <tr><th>Column</th><th>Type</th></tr>
     *   <tr><td>SubmissionId</td><td>Integer (Primary Key)</td></tr>
     *   <tr><td>UserId</td><td>Integer</td></tr>
     *   <tr><td>ExerciseId</td><td>Integer</td></tr>
     *   <tr><td>SubmissionTime</td><td>Integer</td></tr>
     * </table>
     *
     * <p>
     * <table>
     *   <caption><em>Table name: <strong>QuestionGrade</strong></em></caption>
     *   <tr><th>Column</th><th>Type</th></tr>
     *   <tr><td>SubmissionId</td><td>Integer</td></tr>
     *   <tr><td>QuestionId</td><td>Integer</td></tr>
     *   <tr><td>Grade</td><td>Real</td></tr>
     * </table>
     * In this table the combination of SubmissionId and QuestionId together comprise the primary key.
     *
     * @param dburl The JDBC url of the database to open (will be of the form "jdbc:sqlite:...")
     * @return the new connection
     * @throws SQLException
     */
    public Connection openDB(String dburl) throws SQLException {
        this.db = DriverManager.getConnection(dburl);
        Statement st = this.db.createStatement();
        st.executeUpdate("CREATE TABLE IF NOT EXISTS User (UserId integer PRIMARY KEY ,Username text UNIQUE,Firstname text,Lastname text,Password text);");
        st.executeUpdate("CREATE TABLE IF NOT EXISTS Exercise (ExerciseId integer PRIMARY KEY,Name text,DueDate integer);");
        st.executeUpdate("CREATE TABLE IF NOT EXISTS Question (ExerciseId integer  ,QuestionId integer,Name text,Desc text,Points integer,PRIMARY KEY(ExerciseId,QuestionId));");
        st.executeUpdate("CREATE TABLE IF NOT EXISTS Submission (SubmissionId integer PRIMARY KEY,UserId integer,ExerciseId integer,SubmissionTime integer);");
        st.executeUpdate("CREATE TABLE IF NOT EXISTS QuestionGrade (SubmissionId integer ,QuestionId integer,Grade real,PRIMARY KEY(SubmissionId,QuestionId));");
        st.close();
        return this.db;
    }


    /**
     * Close the DB if it is open.
     *
     * @throws SQLException
     */
    public void closeDB() throws SQLException {
        if (db != null) {
            db.close();
            db = null;
        }
    }

    // =========== User Management =============

    /**
     * Add a user to the database / modify an existing user.
     * <p>
     * Add the user to the database if they don't exist. If a user with user.username does exist,
     * update their password and firstname/lastname in the database.
     *
     * @param user
     * @param password
     * @return the userid.
     * @throws SQLException
     */
    public int addOrUpdateUser(User user, String password) throws SQLException {
        PreparedStatement p = this.db.prepareStatement("SELECT * FROM User where Username=?");//look for a given user assumption - username is unique
        p.setString(1, user.username);
        ResultSet res = p.executeQuery();
        if (!res.next()) {//didn't found the user -> create it.
            String sql = "INSERT INTO User(Username,Firstname,Lastname,Password) VALUES(?,?,?,?)";//using ? to prevent sql injection
            p = this.db.prepareStatement(sql);
            p.setString(1, user.username);
            p.setString(2, user.firstname);
            p.setString(3, user.lastname);
            p.setString(4, password);
            p.executeUpdate();
        } else {
            String sql = "UPDATE User SET Firstname=?,Lastname=?,Password=? WHERE UserId=" + res.getInt("UserId");//using the id of user found in the last query
            p = this.db.prepareStatement(sql);
            p.setString(1, user.firstname);
            p.setString(2, user.lastname);
            p.setString(3, password);
            p.executeUpdate();
            return res.getInt("UserId");//the id from the query above didn't change
        }//in case a user was created - look for his id using the username entered(unique)
        p = this.db.prepareStatement("SELECT * FROM User where Username=?");
        p.setString(1, user.username);
        res = p.executeQuery();
        return res.getInt("UserId");
    }


    /**
     * Verify a user's login credentials.
     *
     * @param username
     * @param password
     * @return true if the user exists in the database and the password matches; false otherwise.
     * @throws SQLException <p>
     *                      Note: this is totally insecure. For real-life password checking, it's important to store only
     *                      a password hash
     * @see <a href="https://crackstation.net/hashing-security.htm">How to Hash Passwords Properly</a>
     */
    public boolean verifyLogin(String username, String password) throws SQLException {
        PreparedStatement p = this.db.prepareStatement("SELECT * FROM User where Username=?");//searching for user by username - unique username
        p.setString(1, username);
        ResultSet res = p.executeQuery();
        if (!res.next())//no such user
            return false;
        if (res.getString("Password").equals(password))//user exists and the given password is the correct one
            return true;
        return false;
    }

    // =========== Exercise Management =============

    /**
     * Add an exercise to the database.
     *
     * @param exercise
     * @return the new exercise id, or -1 if an exercise with this id already existed in the database.
     * @throws SQLException
     */
    public int addExercise(Exercise exercise) throws SQLException {
        PreparedStatement p = this.db.prepareStatement("SELECT * FROM Exercise where ExerciseId=?");
        p.setInt(1, exercise.id);
        ResultSet res = p.executeQuery();
        if (res.next()) {//found the exercise -> return -1;
            return -1;
        } else {
            String sql = "INSERT INTO Exercise(ExerciseId,Name,DueDate) VALUES(?,?,?)";
            p = this.db.prepareStatement(sql);
            p.setInt(1, exercise.id);
            p.setString(2, exercise.name);
            p.setLong(3, (Long) exercise.dueDate.getTime());//get the due time in milisecondes from Date type;
            p.executeUpdate();
            int mone = 0;
            for (Exercise.Question q : exercise.questions) {//create the questions from the list for Question type under the Exercise Type
                sql = "INSERT INTO Question(ExerciseId,QuestionId,Name,Desc,Points) VALUES(?,?,?,?,?)";
                p = this.db.prepareStatement(sql);
                p.setInt(1, exercise.id);
                p.setInt(2, mone++);
                p.setString(3, q.name);
                p.setString(4, q.desc);
                p.setInt(5, q.points);
                p.executeUpdate();
            }
            return exercise.id;
        }
    }

    /**
     * Return a list of all the exercises in the database.
     * <p>
     * The list should be sorted by exercise id.
     *
     * @return list of all exercises.
     * @throws SQLException
     */
    public List<Exercise> loadExercises() throws SQLException {
        List<Exercise> listResult = new ArrayList<>();
        Statement st = this.db.createStatement();
        ResultSet res = st.executeQuery("SELECT * FROM Exercise ORDER BY ExerciseId"); //add the exercise sorted by Ids as required
        while (res.next()) {
            Exercise ex = new Exercise(res.getInt("ExerciseId"), res.getString("Name"), new Date(res.getInt("DueDate")));
            st = this.db.createStatement();
            ResultSet listOfQuestionsOfTheExercise = st.executeQuery("SELECT * FROM Question WHERE ExerciseId=" + res.getInt("ExerciseId"));//for each Exercise we build a list of Questions
            while (listOfQuestionsOfTheExercise.next()) {
                ex.addQuestion(listOfQuestionsOfTheExercise.getString("Name"),
                        listOfQuestionsOfTheExercise.getString("Desc"),
                        listOfQuestionsOfTheExercise.getInt("Points"));//using the function given in Exercise module
            }
            listResult.add(ex);//finally add the exercise to the list
        }
        return listResult;
    }

    // ========== Submission Storage ===============

    /**
     * Store a submission in the database.
     * The id field of the submission will be ignored if it is -1.
     * <p>
     * Return -1 if the corresponding user doesn't exist in the database.
     *
     * @param submission
     * @return the submission id.
     * @throws SQLException
     */
    public int storeSubmission(Submission submission) throws SQLException {
        PreparedStatement p = this.db.prepareStatement("SELECT * FROM User where Username=?");
        p.setString(1, submission.user.username);
        ResultSet res = p.executeQuery();
        String sql = "";
        int UserId;
        if (!res.next()) {//didn't found the user -> return -1;
            return -1;
        } else {
            UserId = res.getInt("UserId");
            res.close();
            if (submission.id != -1) {//has an invalid submission Id - enter without an id and will generate auto because it forms the primary key
                sql = "INSERT INTO Submission(SubmissionId,UserId,ExerciseId,SubmissionTime) VALUES(?,?,?,?)";//(SubmissionId integer PRIMARY KEY,UserId integer,ExerciseId integer,SubmissionTime integer)
                p = this.db.prepareStatement(sql);
                p.setInt(1, submission.id);
                p.setInt(2, UserId);
                p.setInt(3, submission.exercise.id);
                p.setLong(4, (long) submission.submissionTime.getTime());

            } else {//has a valid id - enter with the given id
                sql = "INSERT INTO Submission(UserId,ExerciseId,SubmissionTime) VALUES(?,?,?)";//(SubmissionId integer PRIMARY KEY,UserId integer,ExerciseId integer,SubmissionTime integer)
                p = this.db.prepareStatement(sql);
                p.setInt(1, UserId);
                p.setInt(2, submission.exercise.id);
                p.setLong(3, (long) submission.submissionTime.getTime());
            }
            p.executeUpdate();
            int SubmissionId;
            if (submission.id != -1)
                SubmissionId = submission.id;
            else {
                SubmissionId = GetSubmissionId(submission, UserId);//use a handler function below
            }
            for (int i = 0; i < submission.questionGrades.length; i++) {//populate the QuestionGrade DB using the grades from the submission
                sql = "INSERT INTO QuestionGrade(SubmissionId,QuestionId,Grade) VALUES(?,?,?)";
                p = this.db.prepareStatement(sql);
                p.setInt(1, SubmissionId);
                p.setInt(2, i);
                p.setFloat(3, submission.questionGrades[i] / submission.exercise.questions.get(i).points);//claculate the grade by the given format
                p.executeUpdate();
            }
            return SubmissionId;
        }
    }

    /**
     * Gets the SubmissionId from the database with the following info:
     * UserId
     * ExerciseId
     * SubmissionTime
     *
     * we know it's not possible two rows will be back, because no one user can submmit the same Exercise in one specific moment calculated by millisecond
     * if no data found will return -1.
     * <p>
     *
     * @param submission - Submission ,UserId - int
     * @return the submission id.
     * @throws SQLException
     */
    public int GetSubmissionId(Submission submission, int UserId) throws SQLException {
        Statement st = null;
        st = this.db.createStatement();
        //no sql injections by this point - this follows an insert of this specific info.
        ResultSet res = st.executeQuery("SELECT * FROM Submission where UserId=" + UserId +
                " AND ExerciseId=" + submission.exercise.id +
                " AND SubmissionTime=" + (long) submission.submissionTime.getTime());//there couldn't be two submission for the same exercise and the same user at the same time - so we can query with this value and be sure we get one row
        if (!res.next())
            return -1;
        return res.getInt("SubmissionId");
    }

    // ============= Submission Query ===============
    /**
     * Return a prepared SQL statement that, when executed, will
     * return one row for every question of the latest submission for the given exercise by the given user.
     * <p>
     * The rows should be sorted by QuestionId, and each row should contain:
     * - A column named "SubmissionId" with the submission id.
     * - A column named "QuestionId" with the question id,
     * - A column named "Grade" with the grade for that question.
     * - A column named "SubmissionTime" with the time of submission.
     * <p>
     * Parameter 1 of the prepared statement will be set to the User's username, Parameter 2 to the Exercise Id, and
     * Parameter 3 to the number of questions in the given exercise.
     * <p>
     * This will be used by {@link #getLastSubmission(User, Exercise)}
     *
     * @return
     */
    PreparedStatement getLastSubmissionGradesStatement() throws SQLException {
        String sql = "SELECT Submission.SubmissionId,Question.QuestionId,QuestionGrade.Grade,Submission.SubmissionTime " +
                "FROM Question ,Exercise USING(ExerciseId),Submission USING(ExerciseId),User USING(UserId),QuestionGrade USING(submissionId,QuestionId) " +
                "WHERE User.Username=? AND Exercise.ExerciseId=? " +
                "ORDER BY SubmissionTime DESC ,QuestionId " +
                "LIMIT ?";
        //we join the tables by the required info, with the parameters as ? , ordered by the QestionId.
        //the order by of the SubmissionTime is to take the most recent submission of the given exercise and this follows by LIMIT in order to take the amount of rows that equal the amount of questions in exercise.
        PreparedStatement p = this.db.prepareStatement(sql);
        return p;
    }

    /**
     * Return a prepared SQL statement that, when executed, will
     * return one row for every question of the <i>best</i> submission for the given exercise by the given user.
     * The best submission is the one whose point total is maximal.
     * <p>
     * The rows should be sorted by QuestionId, and each row should contain:
     * - A column named "SubmissionId" with the submission id.
     * - A column named "QuestionId" with the question id,
     * - A column named "Grade" with the grade for that question.
     * - A column named "SubmissionTime" with the time of submission.
     * <p>
     * Parameter 1 of the prepared statement will be set to the User's username, Parameter 2 to the Exercise Id, and
     * Parameter 3 to the number of questions in the given exercise.
     * <p>
     * This will be used by {@link #getBestSubmission(User, Exercise)}
     */
    PreparedStatement getBestSubmissionGradesStatement() throws SQLException {

        return null;
    }

    /**
     * Return a submission for the given exercise by the given user that satisfies
     * some condition (as defined by an SQL prepared statement).
     * <p>
     * The prepared statement should accept the user name as parameter 1, the exercise id as parameter 2 and a limit on the
     * number of rows returned as parameter 3, and return a row for each question corresponding to the submission, sorted by questionId.
     * <p>
     * Return null if the user has not submitted the exercise (or is not in the database).
     *
     * @param user
     * @param exercise
     * @param stmt
     * @return
     * @throws SQLException
     */
    Submission getSubmission(User user, Exercise exercise, PreparedStatement stmt) throws SQLException {
        stmt.setString(1, user.username);
        stmt.setInt(2, exercise.id);
        stmt.setInt(3, exercise.questions.size());

        ResultSet res = stmt.executeQuery();

        boolean hasNext = res.next();
        if (!hasNext)
            return null;

        int sid = res.getInt("SubmissionId");
        Date submissionTime = new Date(res.getLong("SubmissionTime"));

        float[] grades = new float[exercise.questions.size()];

        for (int i = 0; hasNext; ++i, hasNext = res.next()) {
            grades[i] = res.getFloat("Grade");
        }

        return new Submission(sid, user, exercise, submissionTime, (float[]) grades);
    }

    /**
     * Return the latest submission for the given exercise by the given user.
     * <p>
     * Return null if the user has not submitted the exercise (or is not in the database).
     *
     * @param user
     * @param exercise
     * @return
     * @throws SQLException
     */
    public Submission getLastSubmission(User user, Exercise exercise) throws SQLException {
        return getSubmission(user, exercise, getLastSubmissionGradesStatement());
    }


    /**
     * Return the submission with the highest total grade
     *
     * @param user     the user for which we retrieve the best submission
     * @param exercise the exercise for which we retrieve the best submission
     * @return
     * @throws SQLException
     */
    public Submission getBestSubmission(User user, Exercise exercise) throws SQLException {
        return getSubmission(user, exercise, getBestSubmissionGradesStatement());
    }
}
