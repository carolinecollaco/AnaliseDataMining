package analisedatamining;

import java.io.IOException;
import java.io.InputStream;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;
import java.sql.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import javax.xml.parsers.ParserConfigurationException;
import org.xml.sax.SAXException;

public class AnaliseDataMining {
    
    public static void main(String[] args) throws Exception{
        
        Connection conexao = abreConexaoComOBanco();
        limpaTabelasSeJaExistem(conexao);
        criaTabelas(conexao); 
        
        List<Pessoa> pessoas = leiaPessoasDoXml();
        List<Conhecido> conhecidos = leiaConhecidosDoXml();
        List<Curtida> curtidas = leiaCurtidasXml();
        
        Set<String> artistasMusicais = pegaArtistasDasCurtidas(curtidas);
        
        inserePessoasNaBase(conexao, pessoas);
        insereConhecidosNaBase(conexao, conhecidos);
        insereArtistasMusicaisNaBase(conexao, artistasMusicais);
        insereCurtidasNaBase(conexao, curtidas);
        
        FazDataMinings.fazDataMinings(conexao);
    }

    private static Set<String> pegaArtistasDasCurtidas(List<Curtida> curtidas) {
        Set<String> artistasMusicais = new LinkedHashSet<>();
        for (Curtida curtida : curtidas) {
            artistasMusicais.add(curtida.bandUri);
        }
        return artistasMusicais;
    }
    
    private static Connection abreConexaoComOBanco() throws SQLException, ClassNotFoundException{
        Class.forName("org.postgresql.Driver");        
        System.out.println("Abrindo conexão em "+DadosDaBase.BASE+" "+DadosDaBase.USUARIO+" "+ DadosDaBase.SENHA);
        return DriverManager.getConnection(DadosDaBase.BASE, DadosDaBase.USUARIO, DadosDaBase.SENHA);
    }
    private static void limpaTabelasSeJaExistem(Connection conexao) throws SQLException {        
        apagaTabelaSeExiste(conexao, "Conhecidos");
        apagaTabelaSeExiste(conexao, "Razao_de_bloqueio");
        apagaTabelaSeExiste(conexao, "Razao");
        apagaTabelaSeExiste(conexao, "Outras");
        apagaTabelaSeExiste(conexao, "Bloqueio");
        apagaTabelaSeExiste(conexao, "Curtidas");
        apagaTabelaSeExiste(conexao, "Musico");
        apagaTabelaSeExiste(conexao, "Artista_musical");
        apagaTabelaSeExiste(conexao, "Pessoas");
    }
    public static void apagaTabelaSeExiste(Connection conexao, String nomeDaTabela) throws SQLException{
        Statement dropTable = conexao.createStatement();
        final String query = "DROP TABLE IF EXISTS "+nomeDaTabela+";";
        System.out.println(query);
        dropTable.execute(query);
        dropTable.close();
    }
    public static void criaTabelas(Connection c) throws SQLException{
        criaTabela(c,"Pessoas",
            "login VARCHAR (200) NOT NULL PRIMARY KEY," +
            "nome VARCHAR (200) NOT NULL,"+
            "cidade VARCHAR (200) NOT NULL"
        );
        
        criaTabela(c,"Conhecidos",
            "pessoa VARCHAR REFERENCES Pessoas," +
            "conhecido VARCHAR REFERENCES Pessoas"          
        );
        
        criaTabela(c,"Razao",
            "razao VARCHAR (200) NOT NULL PRIMARY KEY"         
        );
        
        criaTabela(c,"Bloqueio",
            "id SERIAL NOT NULL PRIMARY KEY," +
            "bloqueador VARCHAR REFERENCES Pessoas,"+
            "bloqueado VARCHAR REFERENCES Pessoas"
        );
         
        criaTabela(c,"Razao_de_bloqueio",
            "bloqueio INT REFERENCES Bloqueio," +
            "razao VARCHAR REFERENCES Razao"          
        );
        
        criaTabela(c,"Outras",
            "bloqueio INT REFERENCES Bloqueio," +
            "explicacao VARCHAR (200)"          
        );
        
        criaTabela(c,"Artista_musical",
            "nome_artistico VARCHAR (200) NOT NULL PRIMARY KEY,"+
            "pais VARCHAR(200)," + 
            "genero VARCHAR (200)," + 
            "tipo VARCHAR"        
                
        );
        
        criaTabela(c,"Musico",
            "nome_real VARCHAR(200)," +
            "estilo_musical VARCHAR(200),"+
            "data_nascimento TIMESTAMP," + 
            "artista VARCHAR REFERENCES Artista_musical"                       
        );
        
         criaTabela(c,"Curtidas",
            "avaliador VARCHAR REFERENCES Pessoas," +
            "avaliado VARCHAR REFERENCES Artista_musical,"+
            "data_nascimento TIMESTAMP," + 
            "nota INT"                       
        );
        
    }
    public static void criaTabela(Connection conexao, String nome, String atributos) throws SQLException {
        Statement createTable = conexao.createStatement();
        System.out.println(createTable);
        createTable.execute("CREATE TABLE "+nome+ " (" +atributos+ ");");
        createTable.close();
    }
    private static List<Pessoa> leiaPessoasDoXml() throws SAXException, IOException, ParserConfigurationException {
        final InputStream resourceAsStream = AnaliseDataMining.class.getResourceAsStream("person.xml");
        DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
        Document docPessoas = dBuilder.parse(resourceAsStream);
        NodeList ListPessoas = docPessoas.getElementsByTagName("Person");
        
        List<Pessoa> pessoas = new ArrayList<>();
        for (int temp = 0; temp < ListPessoas.getLength(); temp++) {

            Node NodePessoas = ListPessoas.item(temp);
            if (NodePessoas.getNodeType() == Node.ELEMENT_NODE) {
                Element eElement = (Element) NodePessoas;

                String name = eElement.getAttribute("name"); 
                String uriTotal = eElement.getAttribute("uri");
                String uri = uriTotal.substring(uriTotal.lastIndexOf("/") + 1);
                String hometown = eElement.getAttribute("hometown");
                
                final Pessoa pessoa = new Pessoa(uri, name, hometown);
                pessoas.add(pessoa);
            }
        }
        
        return pessoas;
    }
    private static List<Conhecido> leiaConhecidosDoXml() throws SAXException, IOException, ParserConfigurationException {
        final InputStream resourceAsStream = AnaliseDataMining.class.getResourceAsStream("knows.xml");
        DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
        Document docConhecidos = dBuilder.parse(resourceAsStream);
        NodeList ListConhecidos = docConhecidos.getElementsByTagName("Knows");
        
        List<Conhecido> conhecidos = new ArrayList<>();
        for (int temp = 0; temp < ListConhecidos.getLength(); temp++) {

            Node NodeConhecidos = ListConhecidos.item(temp);
            if (NodeConhecidos.getNodeType() == Node.ELEMENT_NODE) {
                Element eElement = (Element) NodeConhecidos;

                String personTotal = eElement.getAttribute("person");   
                String person = personTotal.substring(personTotal.lastIndexOf("/") + 1);
                String colleagueTotal = eElement.getAttribute("colleague");   
                String colleague = colleagueTotal.substring(colleagueTotal.lastIndexOf("/") + 1);
                
                final Conhecido conhecido = new Conhecido (person, colleague);
                conhecidos.add(conhecido);
            }
        }        
        return conhecidos;
    }
    private static List<Curtida> leiaCurtidasXml() throws SAXException, IOException, ParserConfigurationException {
        final InputStream resourceAsStream = AnaliseDataMining.class.getResourceAsStream("music.xml");
        DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
        Document docACurtida = dBuilder.parse(resourceAsStream);
        NodeList ListACurtida = docACurtida.getElementsByTagName("LikesMusic");
        
        List<Curtida> curtidas = new ArrayList<>();
        for (int temp = 0; temp < ListACurtida.getLength(); temp++) {

            Node NodeCurtida = ListACurtida.item(temp);
            if (NodeCurtida.getNodeType() == Node.ELEMENT_NODE) {
                Element eElement = (Element) NodeCurtida;

                String personTotal = eElement.getAttribute("person");
                String person = personTotal.substring(personTotal.lastIndexOf("/") + 1);                    
                int rating = Integer.valueOf(eElement.getAttribute("rating"));
                String bandUriTotal = eElement.getAttribute("bandUri");
                String bandUri= bandUriTotal.substring(bandUriTotal.lastIndexOf("/") + 1);
                
                final Curtida artistaMusical = new Curtida (person, rating, bandUri);
                curtidas.add(artistaMusical);
            }
        }        
        return curtidas;
    }
    
    private static void inserePessoasNaBase(Connection conexao, List<Pessoa> pessoas) throws SQLException {
        for (Pessoa pessoa : pessoas) {
            inserePessoaNaBase(conexao, pessoa);
        }
    }
    
    private static void inserePessoaNaBase(Connection conexao, Pessoa pessoa) throws SQLException {
        PreparedStatement statement = conexao.prepareStatement("INSERT INTO Pessoas (login, nome,cidade ) values (?,?,?)");
        statement.setString(1, pessoa.uri);
        statement.setString(2, pessoa.name);
        statement.setString(3, pessoa.hometown);
        System.out.println(statement.toString());
        statement.execute();
        statement.close();
    }

    private static void insereConhecidosNaBase(Connection conexao, List<Conhecido> conhecidos) throws SQLException {
        for (Conhecido conhecido : conhecidos) {
            insereConhecido(conexao, conhecido);
        }
    }
    
    private static void insereConhecido(Connection conexao, Conhecido conhecido) throws SQLException {
        PreparedStatement statement = conexao.prepareStatement("INSERT INTO Conhecidos (pessoa, conhecido) values (?,?)");
        statement.setString(1, conhecido.person);
        statement.setString(2, conhecido.collegue);
        System.out.println(statement);
        statement.execute();
        statement.close();
    }
    
    private static void insereArtistasMusicaisNaBase(Connection conexao, Set<String> artistas) throws SQLException {
        for (String artista : artistas) {
            insereArtistaMusical(conexao, artista);
        }
    }
    
    private static void insereArtistaMusical(Connection conexao, String artista) throws SQLException {
        PreparedStatement statement = conexao.prepareStatement("INSERT INTO Artista_musical (nome_artistico) values (?)");
        statement.setString(1, artista);                
        System.out.println(statement);
        statement.execute();
        statement.close();
    }
  
    private static void insereCurtidasNaBase(Connection conexao, List<Curtida> curtidas) throws SQLException {
        for (Curtida curtida : curtidas) {
            insereCurtida(conexao, curtida);
        }
    }
    
    private static void insereCurtida(Connection conexao, Curtida curtida) throws SQLException {
        PreparedStatement statement = conexao.prepareStatement("INSERT INTO Curtidas (avaliador, avaliado, nota) values (?,?,?)");
        statement.setString(1, curtida.person);      
        statement.setString(2,curtida.bandUri);
        statement.setInt(3,curtida.rating);      
        System.out.println(statement);
        statement.execute();
        statement.close();
    }
    
   
}
