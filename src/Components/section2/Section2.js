import React from "react";
import a from './2.jpg';
import b from './3.png';
import c from './4.jpg';
import d from './5.png';
import e from './6.jpg';
import f from './7.jpg';
import g from './8.jpg';
import h from './9.jpg';
import i from './1.png';
import {Link} from "react-router-dom";


const Section2 = () => {
    return (
        <div className="section">
            <div className="experience">
            <a name="target2"></a>
                <p className="title">List of Members<span className="year">2021.xx.xx ~</span></p>
                <div className="contents1">
                <img className="photo" src={a} alt="agent"/>
                <Link to={"/Detail1"}>
                    <a><div id="data" className="click">Agent1</div><br/><div id="pw">ID : <br/>NAME : </div><div id="view">View details</div><br/></a>
                </Link>
                    <img className="photo" src={b} alt="agent"/>
                <Link to={"/Detail2"}>
                    <a><div id="data" className="click">Agent2</div><br/><div id="pw">ID : <br/>NAME : </div><div id="view">View details</div><br/></a>
                </Link>
                    <img className="photo" src={c} alt="agent"/>
                <Link to={"/Detail3"}>    
                    <a><div id="data" className="click">Agent3</div><br/><div id="pw">ID : <br/>NAME : </div><div id="view">View details</div><br/></a>
                </Link>
                </div>
                <div className="contents2">
                    <img className="photo" src={d} alt="agent"/>
                <Link to={"/Detail4"}>
                    <a><div id="data" className="click">Agent4</div><br/><div id="pw">ID : <br/>NAME : </div><div id="view">View details</div><br/></a>
                </Link>
                    <img className="photo" src={e} alt="agent"/>
                <Link to={"/Detail5"}>        
                    <a><div id="data" className="click">Agent5</div><br/><div id="pw">ID : <br/>NAME : </div><div id="view">View details</div><br/></a>
                </Link>
                    <img className="photo" src={f} alt="agent"/>
                <Link to={"/Detail6"}>
                    <a><div id="data" className="click">Agent6</div><br/><div id="pw">ID : <br/>NAME : </div><div id="view">View details</div><br/></a>
                </Link>
                </div>
                <div className="contents3">
                    <img className="photo" src={g} alt="agent"/>
                <Link to={"/Detail7"}>
                    <a><div id="data" className="click">Agent7</div><br/><div id="pw">ID : <br/>NAME : </div><div id="view">View details</div><br/></a>
                </Link>    
                    <img className="photo" src={h} alt="agent"/>
                <Link to={"/Detail8"}>        
                    <a><div id="data" className="click">Agent8</div><br/><div id="pw">ID : <br/>NAME : </div><div id="view">View details</div><br/></a>
                </Link>    
                    <img className="photo" src={i} alt="agent"/>
                <Link to={"/Detail9"}>
                    <a><div id="data" className="click">Agent9</div><br/><div id="pw">ID : <br/>NAME : </div><div id="view">View details</div><br/></a>
                </Link>
                    <p/>
                </div>
            </div>
            <hr className="featurette-divider"/>
        </div>
    )
}

export default Section2